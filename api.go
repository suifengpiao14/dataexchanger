package datacenter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/logger"
	"github.com/suifengpiao14/datacenter/module/db"
	"github.com/suifengpiao14/datacenter/module/gsjson"
	"github.com/suifengpiao14/datacenter/module/sqltpl"
	datatemplate "github.com/suifengpiao14/datacenter/module/template"
	"github.com/suifengpiao14/datacenter/module/tengocontext"
	"github.com/suifengpiao14/datacenter/util"
	"github.com/suifengpiao14/jsonschemaline"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"
	gormLogger "gorm.io/gorm/logger"
)

// 容器，包含所有预备的资源、脚本等
type Container struct {
	apis     map[string]*apiCompiled
	lockCApi sync.Mutex
}

func NewContainer() *Container {
	return &Container{
		apis:     map[string]*apiCompiled{},
		lockCApi: sync.Mutex{},
	}
}

func (c *Container) RegisterAPI(capi *apiCompiled) {
	c.lockCApi.Lock()
	defer c.lockCApi.Unlock()
	methods := make([]string, 0)
	if capi.Methods != "" {
		methods = strings.Split(capi.Methods, ",")
	}
	capi._container = c // 关联容器
	for _, method := range methods {
		key := apiMapKey(capi.Route, method)
		c.apis[key] = capi
	}
}

// 计算api map key
func apiMapKey(route, method string) (key string) {
	key = strings.ToLower(fmt.Sprintf("%s_%s", route, method))
	return key
}

func (c *Container) GetCApi(route string, method string) (capi *apiCompiled, ok bool) {
	key := apiMapKey(route, method)
	c.lockCApi.Lock()
	defer c.lockCApi.Unlock()
	capi, ok = c.apis[key]
	return capi, ok
}

const (
	SOURCE_DRIVER_MYSQL    = "mysql"
	SOURCE_DRIVER_RABBITMQ = "rabbitmq"
	SOURCE_DRIVER_REDIS    = "redis"
	SOURCE_DRIVER_HTTP     = "http"
	SOURCE_DRIVER_TEMPLATE = "template"
)
const (
	VARIABLE_NAME_CTX         = "ctx"
	VARIABLE_NAME_INPUT       = "input"
	VARIABLE_NAME_PRE_OUT     = "preOut"
	VARIABLE_NAME_MAIN_OUTPUT = "output"
	VARIABLE_NAME_POST_OUTPUT = "postOut"
	VARIABLE_NAME_OUTPUT      = "__output__"
)

type ContextKeyType string

const (
	CONTEXT_KEY_INPUT = ContextKeyType(VARIABLE_NAME_INPUT)
)

type API struct {
	Methods          string `json"methods"`
	Route            string `json"route"`            // 路由,唯一
	BeforeEvent      string `json"beforeEvent"`      // 执行前异步事件
	InputLineSchema  string `json"inputLineSchema"`  // 输入格式化规则
	OutputLineSchema string `json"outputLineSchema"` // 输出格式化规则
	PreScript        string `json"preScript"`        // 前置脚本(如提前验证)
	MainScript       string `json"mainScript"`       // 主脚本
	PostScript       string `json"postScript"`       // 后置脚本(后置脚本异步执行)
	AfterEvent       string `json"afterEvent"`       // 异步事件

}

type apiCompiled struct {
	Route            string `json"route"`
	Methods          string
	_preScript       *tengo.Compiled
	_mainScript      *tengo.Compiled
	_postScript      *tengo.Compiled
	defaultJson      string
	InputSchema      *gojsonschema.JSONLoader
	inputGjsonPath   string
	inputLineSchema  *jsonschemaline.Jsonschemaline
	OutputDefault    string
	OutputSchema     *gojsonschema.JSONLoader
	outputGjsonPath  string
	outputLineSchema *jsonschemaline.Jsonschemaline
	sources          map[string]SourceInterface
	Template         *template.Template
	_container       *Container
}

// 确保多协程安全
func (capi *apiCompiled) GetPreScript() *tengo.Compiled {
	if capi._preScript == nil {
		return nil
	}
	return capi._preScript.Clone()
}

// 确保多协程安全
func (capi *apiCompiled) GetMainScript() *tengo.Compiled {
	if capi._mainScript == nil {
		return nil
	}
	return capi._mainScript.Clone()
}

// 确保多协程安全
func (capi *apiCompiled) GetPostScript() *tengo.Compiled {
	if capi._postScript == nil {
		return nil
	}
	return capi._postScript.Clone()
}

func (capi *apiCompiled) WithTemplate() (self *apiCompiled) {
	capi.Template = template.New("").Funcs(sqltpl.TemplatefuncMap).Funcs(sprig.TxtFuncMap())
	return capi
}

func (capi *apiCompiled) WithFuncMap(funMap template.FuncMap) (self *apiCompiled) {
	if capi.Template == nil {
		capi.WithTemplate()
	}
	capi.Template.Funcs(funMap)
	return capi
}
func (capi *apiCompiled) WithSQLFuncMap() (self *apiCompiled) {
	if capi.Template == nil {
		capi.WithTemplate()
	}
	capi.Template.Funcs(sqltpl.TemplatefuncMap)
	return capi
}

func (capi *apiCompiled) WithCURLFuncMap() (self *apiCompiled) {
	if capi.Template == nil {
		capi.WithTemplate()
	}
	// todo
	return capi
}

//AddTpl 增加模板,同时关联模板执行器
func (capi *apiCompiled) AddTpl(name string, s string, source SourceInterface) (self *apiCompiled) {
	if capi.Template == nil {
		capi.WithTemplate().WithSQLFuncMap().WithCURLFuncMap()
	}
	tmpl := capi.Template.Lookup(name)
	if tmpl == nil {
		tmpl = capi.Template.New(name)
	}
	template.Must(tmpl.Parse(s)) // 追加
	tmp := template.Must(template.New(name).Parse(s))
	tplNames := GetTemplateNames(tmp)
	for _, tplName := range tplNames {
		capi.WithSource(tplName, source)
	}
	return capi
}

func (capi *apiCompiled) WithSource(tplName string, source SourceInterface) {
	if capi.sources == nil {
		capi.sources = make(map[string]SourceInterface)
	}
	capi.sources[tplName] = source
}

func NewApiCompiled(api *API) (capi *apiCompiled, err error) {
	capi = &apiCompiled{
		Methods: api.Methods,
		Route:   api.Route,
	}
	if api.InputLineSchema != "" {
		inputLineschema, err := jsonschemaline.ParseJsonschemaline(api.InputLineSchema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseJsonschemaline.InputLineSchema")
			return nil, err
		}
		capi.inputLineSchema = inputLineschema
		inputSchema, err := inputLineschema.JsonSchema()
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.JsonSchema.InputLineSchema")
			return nil, err
		}
		inputSchemaLoader := gojsonschema.NewStringLoader(string(inputSchema))
		capi.InputSchema = &inputSchemaLoader
		defaultInputJson, err := jsonschemaline.ParseDefaultJson(*inputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.InputLineSchema")
			return nil, err
		}
		capi.defaultJson = defaultInputJson.Json
		capi.inputGjsonPath = inputLineschema.GjsonPath(func(format, src string, item *jsonschemaline.JsonschemalineItem) (path string) {
			switch format {
			case "number", "int", "integer", "float":
				return fmt.Sprintf("%s.@tonum", src)
			}
			return src
		})

	}

	if api.OutputLineSchema != "" {
		outputLineschema, err := jsonschemaline.ParseJsonschemaline(api.OutputLineSchema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseJsonschemaline.OutputLineSchema")
			return nil, err
		}
		capi.outputLineSchema = outputLineschema
		outputSchema, err := outputLineschema.JsonSchema()
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.JsonSchema.OutputLineSchema")
			return nil, err
		}
		outputSchemaLoader := gojsonschema.NewStringLoader(string(outputSchema))
		capi.OutputSchema = &outputSchemaLoader
		defaultOutputJson, err := jsonschemaline.ParseDefaultJson(*outputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.OutputLineSchema")
			return nil, err
		}
		capi.OutputDefault = defaultOutputJson.Json
		capi.outputGjsonPath = outputLineschema.GjsonPath(nil)
	}

	if api.PreScript != "" {
		c, err := capi.compileScript(api.PreScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PreScript")
			return nil, err
		}
		capi._preScript = c
	}

	if api.MainScript != "" {
		c, err := capi.compileScript(api.MainScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.MainScript")
			return nil, err
		}
		capi._mainScript = c
	}

	if api.PostScript != "" {
		c, err := capi.compileScript(api.PostScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PostScript")
			return nil, err
		}
		capi._postScript = c
	}
	return capi, nil
}

func (capi *apiCompiled) ExecSQLTPL(args ...tengo.Object) (tplOut tengo.Object, err error) {
	argLen := len(args)
	if argLen != 3 {
		return nil, tengo.ErrWrongNumArguments
	}
	ctxObj := args[0]
	ctx, ok := ctxObj.(*tengocontext.TengoContext)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "context",
			Expected: "context.Context",
			Found:    ctxObj.TypeName(),
		}
	}
	sqlLogInfo := db.SQLLogInfo{
		Context: ctx.Context,
		Name:    "ExecSQLTPL",
	}
	sqlTplNameObj := args[1]
	var sqlStr string
	sqlTplName, ok := tengo.ToString(sqlTplNameObj)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "sqlTplName",
			Expected: "string",
			Found:    sqlTplNameObj.TypeName(),
		}
	}

	tengoMap, ok := args[2].(*tengo.Map)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    tengoMap.TypeName(),
		}
	}
	volume := &datatemplate.VolumeMap{}
	for k, v := range tengoMap.Value {
		volume.SetValue(k, tengo.ToInterface(v))
	}

	var b bytes.Buffer
	err = capi.Template.ExecuteTemplate(&b, sqlTplName, volume)
	if err != nil {
		err = errors.WithStack(err)
		return tengo.UndefinedValue, err
	}
	namedSQL := strings.ReplaceAll(b.String(), datatemplate.WINDOW_EOF, datatemplate.EOF)
	namedSQL = util.TrimSpaces(namedSQL)

	statment, arguments, err := sqlx.Named(namedSQL, *volume)
	sqlLogInfo.Named = statment
	sqlLogInfo.Data = volume
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	sqlStr = gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
	sqlLogInfo.SQL = sqlStr

	source, ok := capi.sources[sqlTplName]
	if !ok {
		err = errors.Errorf("not found db by tpl name:%s", sqlTplName)
		return nil, err
	}
	out, err := source.Exec(sqlStr)
	if err != nil {
		return tengo.UndefinedValue, err
	}
	sqlLogInfo.Result = out
	logger.SendLogInfo("ExecSQLTPL", sqlLogInfo)
	tplOut = &tengo.String{Value: out}
	return tplOut, nil
}

func (capi *apiCompiled) compileScript(script string) (c *tengo.Compiled, err error) {
	script = fmt.Sprintf(`%s:=func(){%s}()`, VARIABLE_NAME_OUTPUT, script)
	s := tengo.NewScript([]byte(script))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	if err = s.Add("gsjson", gsjson.GSjson); err != nil {
		return nil, err
	}
	if err = s.Add("execSQLTPL", capi.ExecSQLTPL); err != nil {
		return nil, err
	}
	ctx := &tengocontext.TengoContext{
		Context: context.TODO(),
	}
	if err = s.Add(VARIABLE_NAME_CTX, ctx); err != nil {
		return nil, err
	}
	if err = s.Add(VARIABLE_NAME_INPUT, map[string]interface{}{}); err != nil {
		return nil, err
	}
	if err = s.Add(VARIABLE_NAME_PRE_OUT, map[string]interface{}{}); err != nil {
		return nil, err
	}
	if err = s.Add(VARIABLE_NAME_MAIN_OUTPUT, map[string]interface{}{}); err != nil {
		return nil, err
	}
	if err = s.Add(VARIABLE_NAME_POST_OUTPUT, map[string]interface{}{}); err != nil {
		return nil, err
	}
	c, err = s.Compile()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Run 执行API
func (capi *apiCompiled) Run(ctx context.Context, inputJson string) (out string, err error) {

	// 验证参数
	if capi.InputSchema != nil {
		err = Validate(inputJson, *capi.InputSchema)
		if err != nil {
			return "", err
		}
	}
	// 合并默认值
	if capi.defaultJson != "" {
		inputJson, err = jsonschemaline.JsonMerge(capi.defaultJson, inputJson)
		if err != nil {
			return "", err
		}
	}
	if inputJson != "" && capi.inputGjsonPath != "" { // 初步格式化入参(转换成脚本中的输入)
		fmtInupt := fmt.Sprintf(`{"%s":%s}`, capi.inputLineSchema.Meta.ID, inputJson)
		inputJson = gjson.Get(fmtInupt, capi.inputGjsonPath).String()
	}
	var input interface{}
	err = json.Unmarshal([]byte(inputJson), &input)
	if err != nil {
		err = errors.WithMessage(err, "json.Unmarshal(inputJson)")
		return "", err
	}
	ctx = context.WithValue(ctx, CONTEXT_KEY_INPUT, input) //增加输入到上下文
	var preOut interface{}
	ctxObj := &tengocontext.TengoContext{
		Context: ctx,
	}
	if c := capi.GetPreScript(); c != nil {
		if err = c.Set(VARIABLE_NAME_CTX, ctxObj); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetCtx.PreScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_INPUT, input); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetInput.PreScript")
			return "", err
		}
		err = c.Run()
		if err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.PreScript")
			return "", err
		}
		preOutObj := c.Get(VARIABLE_NAME_OUTPUT)
		if err = preOutObj.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetOut.PreScript")
			return "", err
		}
		preOut = preOutObj.Value()
		inputObj := c.Get(VARIABLE_NAME_INPUT)
		if err = inputObj.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetInput.PreScript")
			return "", err
		}
		input = inputObj.Value()
	}
	var mainOut string
	var mainOutObj *tengo.Variable
	if c := capi.GetMainScript(); c != nil {
		if err = c.Set(VARIABLE_NAME_CTX, ctxObj); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetCtx.MainScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_INPUT, input); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetInput.MainScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_PRE_OUT, preOut); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetPreOut.MainScript")
			return "", err
		}
		if err = c.Run(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.MainScript")
			return "", err
		}

		mainOutObj = c.Get(VARIABLE_NAME_OUTPUT)
		if err = mainOutObj.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetOut.MainScript")
			return "", err
		}
		if mainOutObj.ValueType() == "string" {
			mainOut = mainOutObj.String()
		} else {
			b, err := json.Marshal(mainOutObj.Value())
			if err != nil {
				err = errors.WithMessage(err, "apiCompiled.MainScript.out.jsnon.Marshal")
				return "", err
			}
			mainOut = string(b)
		}
	}
	if c := capi.GetPostScript(); c != nil {
		if err = c.Set(VARIABLE_NAME_CTX, ctxObj); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetCtx.PostScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_INPUT, inputJson); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetInput.PostScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_PRE_OUT, preOut); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetPreOut.PostScript")
			return "", err
		}
		if err = c.Set(VARIABLE_NAME_MAIN_OUTPUT, mainOut); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetOut.PostScript")
			return "", err
		}
		if err = c.Run(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.PostScript")
			return "", err
		}
		postOut := c.Get(VARIABLE_NAME_OUTPUT)
		if err = postOut.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetOut.PostScript")
			return "", err
		}
	}
	if mainOut != "" && capi.outputGjsonPath != "" {
		out = gjson.Get(mainOut, capi.outputGjsonPath).String()
	}

	return out, nil
}
