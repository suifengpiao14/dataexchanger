package datacenter

import (
	"sync"
	"text/template"

	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/jsonschemaline"
	"github.com/xeipuuv/gojsonschema"
)

//容器，包含所有预备的资源、脚本等
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

func (c *container) Register(api *apiCompiled) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.apis[api.Route] = api
}

const (
	SOURCE_DRIVER_MYSQL    = "mysql"
	SOURCE_DRIVER_RABBITMQ = "rabbitmq"
	SOURCE_DRIVER_REDIS    = "redis"
	SOURCE_DRIVER_HTTP     = "http"
	SOURCE_DRIVER_TEMPLATE = "template"
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
	Route              string `json:"route"`              // 路由,唯一
	BeforeEvent        string `json:"beforeEvent"`        // 执行前异步事件
	InputLineSchema    string `json:"inputLineSchema"`    // 输入格式化规则
	OutputLineSchema   string `json:"outputLineSchema"`   // 输出格式化规则
	DynamicValidScript string `json:"dynamicValidScript"` // 动态验证脚本(如数据库验证唯一性)
	MainScript         string `json:"mainScript"`         // 主运行脚本
	AfterEvent         string `json:"afterEvent"`         // 异步事件
	Tpl                string `json:"tpl"`                // template 模板
	Source             map[string]SourceInterface
}

type apiCompiled struct {
	Route         string `json:"route"`
	VM            *tengo.Compiled
	InputDefault  string
	InputSchema   *gojsonschema.JSONLoader
	OutputDefault string
	OutputSchema  *gojsonschema.JSONLoader
	Sources       map[string]Source
	Template      *template.Template
}

func NewApiCompiled(api *API) (capi *apiCompiled, err error) {
	capi = &apiCompiled{
		Route: api.Route,
	}
	inputLineschema, err := jsonschemaline.ParseJsonschemaline(api.InputLineSchema)
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.ParseJsonschemaline.InputLineSchema:")
		return nil, err
	}
	inputSchema, err := inputLineschema.JsonSchema()
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.JsonSchema.InputLineSchema:")
		return nil, err
	}
	*capi.InputSchema = gojsonschema.NewStringLoader(string(inputSchema))
	defaultInputJson, err := jsonschemaline.ParseDefaultJson(*inputLineschema)
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.ParseDefaultJson.InputLineSchema:")
		return nil, err
	}
	capi.InputDefault = defaultInputJson.Json

	outputLineschema, err := jsonschemaline.ParseJsonschemaline(api.OutputLineSchema)
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.ParseJsonschemaline.OutputLineSchema:")
		return nil, err
	}
	outputSchema, err := outputLineschema.JsonSchema()
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.JsonSchema.OutputLineSchema:")
		return nil, err
	}
	*capi.OutputSchema = gojsonschema.NewStringLoader(string(outputSchema))
	defaultOutputJson, err := jsonschemaline.ParseDefaultJson(*outputLineschema)
	if err != nil {
		err = errors.WithMessage(err, "NewApiCompiled.ParseDefaultJson.OutputLineSchema:")
		return nil, err
	}
	capi.OutputDefault = defaultOutputJson.Json

	return

}

//Run 执行API
func (api *apiCompiled) Run() (out string, err error) {

	return
}
