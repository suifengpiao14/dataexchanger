package dataexchanger

import (
	"context"
	"fmt"
	"strings"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	tengojson "github.com/d5/tengo/v2/stdlib/json"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/gojsonschemavalidator"
	"github.com/suifengpiao14/jsonschemaline"
	"github.com/suifengpiao14/tengolib"
	"github.com/suifengpiao14/tengolib/tengocontext"
	"github.com/suifengpiao14/tengolib/tengodb"
	"github.com/suifengpiao14/tengolib/tengogsjson"
	"github.com/suifengpiao14/tengolib/tengologger"
	"github.com/suifengpiao14/tengolib/tengosource"
	"github.com/suifengpiao14/tengolib/tengotemplate"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/xeipuuv/gojsonschema"
)

const (
	VARIABLE_STORAGE = "storage"
)

type ContextKeyType string

const (
	CONTEXT_KEY_STORAGE = ContextKeyType(VARIABLE_STORAGE)
)

// DtoAPI 外部接收参数 dto
type DtoAPI struct {
	Methods          string `json:"methods"`
	Route            string `json:"route"`            // 路由,唯一
	BeforeEvent      string `json:"beforeEvent"`      // 执行前异步事件
	InputLineSchema  string `json:"inputLineSchema"`  // 输入格式化规则
	OutputLineSchema string `json:"outputLineSchema"` // 输出格式化规则
	PreScript        string `json:"preScript"`        // 前置脚本(如提前验证)
	MainScript       string `json:"mainScript"`       // 主脚本
	PostScript       string `json:"postScript"`       // 后置脚本(后置脚本异步执行)
	AfterEvent       string `json:"afterEvent"`       // 异步事件

}

type apiCompiled struct {
	Route            string `json:"route"`
	Methods          string
	_preScript       *tengo.Compiled
	_mainScript      *tengo.Compiled
	_postScript      *tengo.Compiled
	defaultJson      string
	inputSchema      *gojsonschema.JSONLoader
	inputGjsonPath   string
	inputLineSchema  *jsonschemaline.Jsonschemaline
	outputDefault    string
	outputSchema     *gojsonschema.JSONLoader
	outputGjsonPath  string
	outputLineSchema *jsonschemaline.Jsonschemaline
	sourcePool       *tengosource.SourcePool
	template         *tengotemplate.TengoTemplate
	_container       *Container
}

func NewApiCompiled(api *DtoAPI) (capi *apiCompiled, err error) {
	capi = &apiCompiled{
		Methods:    api.Methods,
		Route:      api.Route,
		sourcePool: tengosource.NewSourcePool(),
		template:   tengotemplate.NewTemplate(),
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
		capi.inputSchema = &inputSchemaLoader
		defaultInputJson, err := jsonschemaline.ParseDefaultJson(*inputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.InputLineSchema")
			return nil, err
		}
		capi.defaultJson = defaultInputJson.Json
		capi.inputGjsonPath = inputLineschema.GjsonPath(func(format, src string, item *jsonschemaline.JsonschemalineItem) (path string) {
			format = strings.ToLower(format)
			path = src // 默认值
			switch format {
			case "int", "number", "integer", "float":
				path = fmt.Sprintf("%s.@tonum", src)
			}
			return
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
		capi.outputSchema = &outputSchemaLoader
		defaultOutputJson, err := jsonschemaline.ParseDefaultJson(*outputLineschema)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.OutputLineSchema")
			return nil, err
		}
		capi.outputDefault = defaultOutputJson.Json
		capi.outputGjsonPath = outputLineschema.GjsonPath(func(format, src string, item *jsonschemaline.JsonschemalineItem) (path string) {
			typ := strings.ToLower(item.Type)
			path = src // 默认值
			switch typ {
			case "string":
				path = fmt.Sprintf("%s.@tostring", src)
			case "int", "number", "integer", "float":
				path = fmt.Sprintf("%s.@tonum", src)
			}
			return path
		})
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

//RegisterTemplateAndRelationSource 注册模板
func (capi *apiCompiled) RegisterTemplate(name string, s string) (tplNames []string, err error) {
	tplNames = capi.template.AddTpl(name, s)
	return tplNames, nil
}

//RelationTemplateAndSource 设置模版依赖的资源
func (capi *apiCompiled) SetTemplateDependSource(templateIdentifers []string, sourceIdentifer string) (err error) {
	for _, tplName := range templateIdentifers {
		err = capi.sourcePool.AddTemplateIdentiferRelation(tplName, sourceIdentifer)
		if err != nil {
			return err
		}
	}
	return nil
}

//RegisterSource 注册所有可能使用到的资源
func (capi *apiCompiled) RegisterSource(s tengosource.Source) (err error) {
	err = capi.sourcePool.RegisterSource(s)
	if err != nil {
		return err
	}
	return nil
}

// Run 执行API
func (capi *apiCompiled) Run(ctx context.Context, inputJson string) (out string, err error) {
	//收集日志
	logInfo := RunLogInfo{
		Name:          LOG_INFO_RUN,
		Context:       ctx,
		OriginalInput: inputJson,
		DefaultJson:   capi.defaultJson,
	}
	defer func() {
		// 发送日志
		logInfo.Err = err
		tengologger.SendLogInfo(logInfo)
	}()
	// 合并默认值
	if capi.defaultJson != "" {
		inputJson, err = jsonschemaline.JsonMerge(capi.defaultJson, inputJson)
		if err != nil {
			return "", err
		}
	}
	// 验证参数
	if capi.inputSchema != nil {
		err = gojsonschemavalidator.Validate(inputJson, *capi.inputSchema)
		if err != nil {
			return "", err
		}
	}
	if inputJson != "" && capi.inputGjsonPath != "" { // 初步格式化入参(转换成脚本中的输入)
		fmtInupt := fmt.Sprintf(`{"%s":%s}`, capi.inputLineSchema.Meta.ID, inputJson)
		inputJson = gjson.Get(fmtInupt, capi.inputGjsonPath).String()
	}
	logInfo.PreInput = inputJson
	inputRootName := string(capi.inputLineSchema.Meta.ID)
	storage := tengogsjson.NewStorage()
	ctx = context.WithValue(ctx, CONTEXT_KEY_STORAGE, storage) //增加存储到上下文
	ctxObj := &tengocontext.TengoContext{
		Context: ctx,
	}
	storage.Ctx = ctxObj                                                               // 记录上下文
	storage.DiskSpace, err = sjson.SetRaw(storage.DiskSpace, inputRootName, inputJson) // 输入也作为输出的一个参考
	storage.Memory = &tengo.String{Value: inputJson}
	if gjson.Valid(inputJson) { //重新修改storage.Memory
		storage.Memory, err = tengojson.Decode([]byte(inputJson))
		for _, item := range capi.inputLineSchema.Items {
			typ := item.Type
			switch typ {
			case "int", "integer", "number":
				//todo 将float64 改成 int
			}
		}
		if err != nil {
			return "", err
		}
	}
	if err != nil {
		err = errors.WithMessage(err, "set input to storage")
		return "", err
	}
	if c := capi.getPreScript(); c != nil {
		logInfo.PreInput = storage.DiskSpace
		if err = c.Set(VARIABLE_STORAGE, storage); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetStorage.PreScript")
			return "", err
		}
		err = c.Run()
		if err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.PreScript")
			return "", err
		}
		logInfo.PreOutput = storage.DiskSpace
	}

	if c := capi.getMainScript(); c != nil {
		if err = c.Set(VARIABLE_STORAGE, storage); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetStorage.MainScript")
			return "", err
		}
		if err = c.Run(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.MainScript")
			return "", err
		}

		logInfo.Out = storage.DiskSpace
	}
	//pos script 异步执行,需要同步处理的需要放到main中
	if c := capi.getPostScript(); c != nil {
		if err = c.Set(VARIABLE_STORAGE, storage); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetStorage.PostScript")
			return "", err
		}

		go func(c *tengo.Compiled, runLogInfo RunLogInfo) {

			// 复制一份,避免多协程竞争写
			var err error
			cpRunLogInfo := RunLogInfo{
				Context:       runLogInfo.Context,
				Name:          LOG_INFO_RUN_POST,
				OriginalInput: runLogInfo.OriginalInput,
				DefaultJson:   runLogInfo.DefaultJson,
				PreInput:      runLogInfo.PreInput,
				PreOutput:     runLogInfo.PreOutput,
				Out:           runLogInfo.Out,
			}
			defer func() {
				if panicInfo := recover(); panicInfo != nil {
					cpRunLogInfo.Err = errors.New(fmt.Sprintf("%v", panicInfo))
					tengologger.SendLogInfo(cpRunLogInfo)
				}
			}()
			defer func() {
				// 发送日志
				cpRunLogInfo.Err = err
				tengologger.SendLogInfo(cpRunLogInfo)
			}()

			if err = c.Run(); err != nil {
				err = errors.WithMessage(err, "apiCompiled.Run.PostScript")
				return
			}
			cpRunLogInfo.PostOut = storage.DiskSpace
		}(c, logInfo)

	}
	scriptOut := storage.DiskSpace
	if scriptOut != "" && capi.outputGjsonPath != "" {
		out = gjson.Get(scriptOut, capi.outputGjsonPath).String()
		rootName := string(capi.outputLineSchema.Meta.ID)
		out = gjson.Get(out, rootName).String()
	}
	return out, nil
}

func (capi *apiCompiled) compileScript(script string) (c *tengo.Compiled, err error) {
	s := tengo.NewScript([]byte(script))
	s.EnableFileImport(true)
	mods := stdlib.GetModuleMap(stdlib.AllModuleNames()...)
	mods2 := tengolib.GetModuleMap(tengolib.AllModuleNames()...)
	mods.AddMap(mods2)
	s.SetImports(mods)
	if err = s.Add("execSQLTPL", capi.execSQLTPL); err != nil {
		return nil, err
	}
	if err = s.Add("getDBByTemplateName", capi.sourcePool.TengoGetProviderByTemplateIdentifer); err != nil {
		return nil, err
	}
	if err = s.Add("execTPL", capi.template.TengoExec); err != nil {
		return nil, err
	}
	gjsonMemory := tengogsjson.NewStorage()
	if err = s.Add(VARIABLE_STORAGE, gjsonMemory); err != nil {
		return nil, err
	}
	c, err = s.Compile()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (capi *apiCompiled) execSQLTPL(args ...tengo.Object) (dbResultTengo tengo.Object, err error) {
	sqlLogInfo := tengodb.LogInfoEXECSQL{}
	defer func() {
		sqlLogInfo.Err = err
		tengologger.SendLogInfo(sqlLogInfo)
	}()
	argLen := len(args)
	if argLen != 3 {
		return nil, tengo.ErrWrongNumArguments
	}
	ctxObjPossible, tplIdentiferObj, dataObj := args[0], args[1], args[2]
	ctxObj, ok := ctxObjPossible.(*tengocontext.TengoContext)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "context",
			Expected: "context.Context",
			Found:    ctxObjPossible.TypeName(),
		}
	}
	ctx := ctxObj.Context

	tplName, ok := tengo.ToString(tplIdentiferObj)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "tplName",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	tengoMap, ok := dataObj.(*tengo.Map)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    args[1].TypeName(),
		}
	}
	volume := &tengotemplate.VolumeMap{}
	for k, v := range tengoMap.Value {
		volume.SetValue(k, tengo.ToInterface(v))
	}

	tplOut, volumeI, err := capi.template.Exec(tplName, volume)
	if err != nil {
		return nil, err
	}
	sqlStr, err := tengotemplate.ToSQL(tplOut, volumeI.ToMap())
	if err != nil {
		return nil, err
	}
	provider, err := capi.sourcePool.GetProviderByTemplateIdentifer(tplName)
	if err != nil {
		return nil, err
	}
	dbProvider, ok := provider.(tengodb.TengoDBInterface)
	if !ok {
		err = errors.Errorf("ExecSQLTPL required db source,got:%s", provider.TypeName())
		return nil, err
	}
	dbResult, err := dbProvider.ExecOrQueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}

	sqlLogInfo.Result = dbResult
	dbResultTengo = &tengo.String{Value: dbResult}
	return dbResultTengo, nil
}

// 确保多协程安全
func (capi *apiCompiled) getPreScript() *tengo.Compiled {
	if capi._preScript == nil {
		return nil
	}
	return capi._preScript.Clone()
}

// 确保多协程安全
func (capi *apiCompiled) getMainScript() *tengo.Compiled {
	if capi._mainScript == nil {
		return nil
	}
	return capi._mainScript.Clone()
}

// 确保多协程安全
func (capi *apiCompiled) getPostScript() *tengo.Compiled {
	if capi._postScript == nil {
		return nil
	}
	return capi._postScript.Clone()
}
