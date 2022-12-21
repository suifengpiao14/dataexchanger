package source

import (
	"encoding/json"

	"github.com/pkg/errors"
)

const (
	PROVIDER_SQL      = "SQL"
	PROVIDER_CURL     = "CURL"
	PROVIDER_BIN      = "BIN"
	PROVIDER_REDIS    = "REDIS"
	PROVIDER_RABBITMQ = "RABBITMQ"
)

type SourceInterface interface {
	Exec(s string) (string, error) // label 用来坐标记，方便分类处理
	GetSource() (handler interface{})
}

//MakeSource 根据名称，获取exec 执行器，后续改成注册执行器方式
func MakeSource(identifier string, configJson string) (execProvider SourceInterface, err error) {

	switch identifier {
	case PROVIDER_SQL:
		var config DBExecProviderConfig
		if configJson != "" {
			err = json.Unmarshal([]byte(configJson), &config)
			if err != nil {
				return nil, err
			}
		}
		execProvider = &DBExecProvider{
			Config: config,
		}
	case PROVIDER_CURL:
		var config CURLExecProviderConfig
		if configJson != "" {
			err = json.Unmarshal([]byte(configJson), &config)
			if err != nil {
				return nil, err
			}
		}
		execProvider = &CURLExecProvider{
			Config: config,
		}
	case PROVIDER_BIN:
		execProvider = &BinExecProvider{}
	default:
		err = errors.Errorf("not suport source type :%s", identifier)
		return nil, err
	}
	return execProvider, nil
}
