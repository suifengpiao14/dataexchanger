package dataexchanger

import "github.com/suifengpiao14/tengolib/tengosource"

const (
	PROVIDER_SQL_MEMORY = tengosource.PROVIDER_SQL_MEMORY
	PROVIDER_SQL        = tengosource.PROVIDER_SQL
	PROVIDER_CURL       = tengosource.PROVIDER_CURL
	PROVIDER_BIN        = tengosource.PROVIDER_BIN
	PROVIDER_REDIS      = tengosource.PROVIDER_REDIS
	PROVIDER_RABBITMQ   = tengosource.PROVIDER_RABBITMQ
)

//MakeSource 简单封装，隐藏包依赖细节
func MakeSource(identifer string, typ string, config string) (s tengosource.Source, err error) {
	return tengosource.MakeSource(identifer, typ, config)
}
