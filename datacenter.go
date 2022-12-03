package datacenter

import (
	"fmt"
	"strings"
	"sync"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/gsjson"
	"github.com/suifengpiao14/jsonschemaline"
	"github.com/xeipuuv/gojsonschema"
)

// 容器，包含所有预备的资源、脚本等
type container struct {
	apis map[string]*apiCompiled
	lock sync.Mutex
}

func NewApiContainer() *container {
	return &container{
		apis: map[string]*apiCompiled{},
		lock: sync.Mutex{},
	}
}

func (c *container) Register(api *API) (err error) {
	capi, err := makeApiCompiled(api)
	if err != nil {
		return err
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, method := range api.Methods {
		key := apiMapKey(api.Route, method)
		c.apis[key] = capi
	}

	return nil
}

// 计算api map key
func apiMapKey(route, method string) (key string) {
	key = strings.ToLower(fmt.Sprintf("%s_%s", route, method))
	return key
}

func (c *container) GetCApi(route string, method string) (capi *apiCompiled, err error) {
	key := apiMapKey(route, method)
	c.lock.Lock()
	defer c.lock.Unlock()
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

type Source struct {
	ID     string `json:"id"`     //资源唯一标识
	Driver string `json:"driver"` //驱动
	Config string `json:"config"` //资源配置
}

type SourceInterface interface {
	GetID() (identifier string)
	Run(s string, label string) (string, error) // label 用来坐标记，方便分类处理
	GetHandler() (handler interface{})
}
type API struct {
	Methods          []string `json:"methods"`
	Route            string   `json:"route"`            // 路由,唯一
	BeforeEvent      string   `json:"beforeEvent"`      // 执行前异步事件
	InputLineSchema  string   `json:"inputLineSchema"`  // 输入格式化规则
	OutputLineSchema string   `json:"outputLineSchema"` // 输出格式化规则
	PreScript        string   `json:"preScript"`        // 前置脚本(如提前验证)
	MainScript       string   `json:"mainScript"`       // 主脚本
	PostScript       string   `json:"postScript"`       // 后置脚本(后置脚本异步执行)
	AfterEvent       string   `json:"afterEvent"`       // 异步事件
	Tpl              string   `json:"tpl"`              // template 模板
	Source           map[string]SourceInterface
}

type apiCompiled struct {
	Route         string `json:"route"`
	_preScript    *tengo.Compiled
	_mainScript   *tengo.Compiled
	_postScript   *tengo.Compiled
	defaultJson   string
	InputSchema   *gojsonschema.JSONLoader
	OutputDefault string
	OutputSchema  *gojsonschema.JSONLoader
	Sources       map[string]SourceInterface
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

func makeApiCompiled(api *API) (capi *apiCompiled, err error) {
	capi = &apiCompiled{
		Route: api.Route,
	}
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
	*capi.InputSchema = gojsonschema.NewStringLoader(string(inputSchema))
	defaultInputJson, err := jsonschemaline.ParseDefaultJson(*inputLineschema)
	if err != nil {
		err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.InputLineSchema:")
		return nil, err
	}
	capi.defaultJson = defaultInputJson.Json

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
	*capi.OutputSchema = gojsonschema.NewStringLoader(string(outputSchema))
	defaultOutputJson, err := jsonschemaline.ParseDefaultJson(*outputLineschema)
	if err != nil {
		err = errors.WithMessage(err, "makeApiCompiled.ParseDefaultJson.OutputLineSchema:")
		return nil, err
	}
	capi.OutputDefault = defaultOutputJson.Json
	if api.PreScript != "" {
		c, err := compileScript(api.PreScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PreScript:")
			return nil, err
		}
		capi._preScript = c
	}

	if api.MainScript != "" {
		c, err := compileScript(api.MainScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.MainScript:")
			return nil, err
		}
		capi._preScript = c
	}

	if api.PostScript != "" {
		c, err := compileScript(api.PostScript)
		if err != nil {
			err = errors.WithMessage(err, "makeApiCompiled.Compiled.PostScript:")
			return nil, err
		}
		capi._postScript = c
	}
	return capi, nil
}

func compileScript(script string) (c *tengo.Compiled, err error) {
	s := fmt.Sprintf(`%s:=func(){%s}()`, VARIABLE_NAME_OUTPUT, script)
	preScript := tengo.NewScript([]byte(s))
	preScript.EnableFileImport(true)
	preScript.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err = preScript.Add("GSjson", gsjson.GSjon)
	if err != nil {
		return nil, err
	}

	preScript.Add("input", struct{}{})
	preScript.Add("preOut", struct{}{})
	preScript.Add("out", struct{}{})
	preScript.Add("postOut", struct{}{})
	c, err = preScript.Compile()
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
	var preOut *tengo.Variable
	if c := capi.GetPreScript(); c != nil {
		if err = c.Set(VARIABLE_NAME_INPUT, inputJson); err != nil {
			err = errors.WithMessage(err, "apiCompiled.SetInput.PreScript")
			return "", err
		}
		err = c.Run()
		if err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.PreScript")
			return "", err
		}
		preOut = c.Get(VARIABLE_NAME_OUTPUT)
		if err = preOut.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetOut.PreScript")
			return "", err
		}
	}
	var mainOut *tengo.Variable
	if c := capi.GetMainScript(); c != nil {
		if err = c.Set(VARIABLE_NAME_INPUT, inputJson); err != nil {
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

		mainOut = c.Get(VARIABLE_NAME_OUTPUT)
		if err = mainOut.Error(); err != nil {
			err = errors.WithMessage(err, "apiCompiled.Run.GetOut.MainScript")
			return "", err
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
	if mainOut != nil {
		out = mainOut.String()
	}
	return out, nil
}
