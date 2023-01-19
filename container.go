package dataexchanger

import (
	"fmt"
	"strings"
	"sync"
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


