package datacenter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/gsjson"
	datatemplate "github.com/suifengpiao14/datacenter/module/template"
	"github.com/suifengpiao14/datacenter/util"
	"github.com/suifengpiao14/jsonschemaline"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/xeipuuv/gojsonschema"
	gormLogger "gorm.io/gorm/logger"
)

// 容器，包含所有预备的资源、脚本等
type container struct {
	apis       map[string]*apiCompiled
	sourcePool *SourcePool
	lockCApi   sync.Mutex
}

func NewContainer() *container {
	return &container{
		apis:       map[string]*apiCompiled{},
		lockCApi:   sync.Mutex{},
		sourcePool: &SourcePool{},
	}
}

func (c *container) RegisterAPI(capi *apiCompiled) (err error) {
	c.lockCApi.Lock()
	defer c.lockCApi.Unlock()
	methods := make([]string, 0)
	if capi.Methods != "" {
		methods = strings.Split(capi.Methods, ",")
	}
	for _, method := range methods {
		key := apiMapKey(capi.Route, method)
		c.apis[key] = capi
	}

	return nil
}

func (c *container) RegisterSource(identifier string, source SourceInterface) (err error) {
	c.sourcePool.RegisterSource(identifier, source)
	return nil
}

// 计算api map key
func apiMapKey(route, method string) (key string) {
	key = strings.ToLower(fmt.Sprintf("%s_%s", route, method))
	return key
}

func (c *container) GetCApi(route string, method string) (capi *apiCompiled, err error) {
	key := apiMapKey(route, method)
	c.lockCApi.Lock()
	defer c.lockCApi.Unlock()
	capi, ok := c.apis[key]
	if !ok {
		err = errors.Errorf("404:4000:not found api by route %s,method:%s", route, method)
		return nil, err
	}
	return capi, nil
}
func (c *container) Handle(route string, method string, inputJson string) (out string, err error) {
	capi, err := c.GetCApi(route, method)
	if err != nil {
		return "", err
	}
	out, err = capi.Run(inputJson)
	if err != nil {
		return "", err
	}
	return out, nil
}

const (
	SOURCE_DRIVER_MYSQL    = "mysql"
	SOURCE_DRIVER_RABBITMQ = "rabbitmq"
	SOURCE_DRIVER_REDIS    = "redis"
	SOURCE_DRIVER_HTTP     = "http"
	SOURCE_DRIVER_TEMPLATE = "template"
)
const (
	VARIABLE_NAME_INPUT       = "input"
	VARIABLE_NAME_PRE_OUT     = "preOut"
	VARIABLE_NAME_MAIN_OUTPUT = "output"
	VARIABLE_NAME_POST_OUTPUT = "postOut"
	VARIABLE_NAME_OUTPUT      = "__output__"
)

type API struct {
	Methods          string `json:"methods"`
	Route            string `json:"route"`            // 路由,唯一
	BeforeEvent      string `json:"beforeEvent"`      // 执行前异步事件
	InputLineSchema  string `json:"inputLineSchema"`  // 输入格式化规则
	OutputLineSchema string `json:"outputLineSchema"` // 输出格式化规则
	PreScript        string `json:"preScript"`        // 前置脚本(如提前验证)
	MainScript       string `json:"mainScript"`       // 主脚本
	PostScript       string `json:"postScript"`       // 后置脚本(后置脚本异步执行)
	AfterEvent       string `json:"afterEvent"`       // 异步事件
	Tpl              string `json:"tpl"`              // template 模板

}

type apiCompiled struct {
	Route          string `json:"route"`
	Methods        string
	_preScript     *tengo.Compiled
	_mainScript    *tengo.Compiled
	_postScript    *tengo.Compiled
	defaultJson    string
	InputSchema    *gojsonschema.JSONLoader
	inputInstruct  *jsonschemaline.InstructTpl
	OutputDefault  string
	OutputSchema   *gojsonschema.JSONLoader
	outputInstruct *jsonschemaline.InstructTpl
	sources        map[string]SourceInterface
	Template       *template.Template
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

func (capi *apiCompiled) WithTemplate(tpl *template.Template) {
	capi.Template = tpl
}

func (capi *apiCompiled) WithSource(tplName string, source SourceInterface) {
	if capi.sources == nil {
		capi.sources = make(map[string]SourceInterface)
	}
	capi.sources[tplName] = source
}

func NewApiCompiled(api *API) (capi *apiCompiled, err error) {
	capi = &apiCompiled{
		Route: api.Route,
	}
	if api.InputLineSchema != "" {
		inputLineschema, err := jsonschemaline.ParseJsonschemaline(api.InputLineSchema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseJsonschemaline.InputLineSchema:")
			return nil, err
		}
		inputSchema, err := inputLineschema.JsonSchema()
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.JsonSchema.InputLineSchema:")
			return nil, err
		}
		inputSchemaLoader := gojsonschema.NewStringLoader(string(inputSchema))
		capi.InputSchema = &inputSchemaLoader
		defaultInputJson, err := jsonschemaline.ParseDefaultJson(*inputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.InputLineSchema:")
			return nil, err
		}
		capi.defaultJson = defaultInputJson.Json
		capi.inputInstruct = jsonschemaline.ParseInstructTp(*inputLineschema)
	}

	if api.OutputLineSchema != "" {
		outputLineschema, err := jsonschemaline.ParseJsonschemaline(api.OutputLineSchema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseJsonschemaline.OutputLineSchema:")
			return nil, err
		}
		outputSchema, err := outputLineschema.JsonSchema()
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.JsonSchema.OutputLineSchema:")
			return nil, err
		}
		outputSchemaLoader := gojsonschema.NewStringLoader(string(outputSchema))
		capi.OutputSchema = &outputSchemaLoader
		defaultOutputJson, err := jsonschemaline.ParseDefaultJson(*outputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.OutputLineSchema:")
			return nil, err
		}
		capi.OutputDefault = defaultOutputJson.Json
		capi.outputInstruct = jsonschemaline.ParseInstructTp(*outputLineschema)
	}

	if api.PreScript != "" {
		c, err := capi.compileScript(api.PreScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PreScript:")
			return nil, err
		}
		capi._preScript = c
	}

	if api.MainScript != "" {
		c, err := capi.compileScript(api.MainScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.MainScript:")
			return nil, err
		}
		capi._mainScript = c
	}

	if api.PostScript != "" {
		c, err := capi.compileScript(api.PostScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PostScript:")
			return nil, err
		}
		capi._postScript = c
	}
	return capi, nil
}

func (capi *apiCompiled) ExecSQLTPL(args ...tengo.Object) (tplOut tengo.Object, err error) {
	argLen := len(args)
	if argLen != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	sqlTplNameObj := args[0]
	var sql string
	sqlTplName, ok := tengo.ToString(sqlTplNameObj)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "sqlTplName",
			Expected: "string",
			Found:    sqlTplNameObj.TypeName(),
		}
	}

	tengoMap, ok := args[1].(*tengo.Map)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    args[1].TypeName(),
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
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	sql = gormLogger.ExplainSQL(statment, nil, `'`, arguments...)

	source, ok := capi.sources[sqlTplName]
	if !ok {
		err = errors.Errorf("not found db by tpl name:%s", sqlTplName)
		return nil, err
	}
	out, err := source.Exec(sql)
	if err != nil {
		return tengo.UndefinedValue, err
	}
	tplOut = &tengo.String{Value: out}
	return tplOut, nil
}

func (capi *apiCompiled) compileScript(script string) (c *tengo.Compiled, err error) {
	script = fmt.Sprintf(`%s:=func(){%s}()`, VARIABLE_NAME_OUTPUT, script)
	s := tengo.NewScript([]byte(script))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err = s.Add("GSjson", gsjson.GSjon)
	if err != nil {
		return nil, err
	}
	err = s.Add("execSQLTPL", capi.ExecSQLTPL)
	if err != nil {
		return nil, err
	}

	s.Add("input", map[string]interface{}{})
	s.Add("preOut", map[string]interface{}{})
	s.Add("out", map[string]interface{}{})
	s.Add("postOut", map[string]interface{}{})
	c, err = s.Compile()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Run 执行API
func (capi *apiCompiled) Run(inputJson string) (out string, err error) {
	if capi.defaultJson != "" {
		inputJson, err = jsonschemaline.JsonMerge(capi.defaultJson, inputJson)
		if err != nil {
			return "", err
		}
	}
	var input interface{}
	err = json.Unmarshal([]byte(inputJson), &input)
	if err != nil {
		err = errors.WithMessage(err, "json.Unmarshal(inputJson)")
		return "", err
	}
	var preOut interface{}
	if c := capi.GetPreScript(); c != nil {
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
	if mainOut != "" && capi.outputInstruct != nil {
		for _, instructTpl := range capi.outputInstruct.Instructs.Unique() {
			v := gjson.Get(mainOut, instructTpl.Src).String()
			out, err = sjson.Set(out, instructTpl.Dst, v)
			if err != nil {
				err = errors.WithMessage(err, "apiCompiled.Run.mainOut.format.output")
				return "", err
			}
		}
	}

	return out, nil
}
