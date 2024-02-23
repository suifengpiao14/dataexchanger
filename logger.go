package dataexchanger

import (
	"context"

	"github.com/suifengpiao14/logchan/v2"
	"github.com/suifengpiao14/tengolib/tengodb"
	"github.com/suifengpiao14/tengolib/tengotemplate"
)

// 收集依赖包中的日志名称，实现封装细节(减少使用方明确引入本包依赖的包，隐藏细节)，同时方便统日志命名规则、方便集中查找管理
/*****************************************统一管理日志start**********************************************/
const (
	LOG_INFO_EXEC_SQL     = tengodb.LOG_INFO_EXEC_SQL
	LOG_INFO_SQL_TEMPLATE = tengotemplate.LOG_INFO_SQL_TEMPLATE
)

type LogInfoEXECSQL tengodb.LogInfoEXECSQL
type LogInfoTemplateSQL tengotemplate.LogInfoTemplateSQL

type LogInforInterface logchan.LogInforInterface

/*****************************************统一管理日志end**********************************************/
const (
	LOG_INFO_RUN      = "apiCompiled.Run"
	LOG_INFO_RUN_POST = "apiCompiled.Run.post"
)

//TryConvert2LogInfoExecSQL log 类型转换,先通过名称确定类型
func TryConvert2LogInfoExecSQL(log logchan.LogInforInterface) (logInfoEXECSQL *tengodb.LogInfoEXECSQL, ok bool) {
	logInfoEXECSQL, ok = log.(*tengodb.LogInfoEXECSQL)
	if ok {
		return logInfoEXECSQL, ok
	}
	tmp, ok := log.(*tengodb.LogInfoEXECSQL)
	if ok {
		logInfoEXECSQL = tmp
	}
	return logInfoEXECSQL, ok
}

//TryConvert2LogInfoSQLTemplate log 类型转换,先通过名称确定类型
func TryConvert2LogInfoSQLTemplate(log logchan.LogInforInterface) (logInfoTemplateSQL *tengotemplate.LogInfoTemplateSQL, ok bool) {
	logInfoTemplateSQL, ok = log.(*tengotemplate.LogInfoTemplateSQL)
	if ok {
		return logInfoTemplateSQL, ok
	}
	tmp, ok := log.(*tengotemplate.LogInfoTemplateSQL)
	if ok {
		logInfoTemplateSQL = tmp
	}
	return logInfoTemplateSQL, ok
}

//TryConvert2LogInfoRunLogInfo log 类型转换,先通过名称确定类型
func TryConvert2LogInfoRunLogInfo(log logchan.LogInforInterface) (logInfoRunLogInfoL *RunLogInfo, ok bool) {
	logInfoRunLogInfoL, ok = log.(*RunLogInfo)
	if ok {
		return logInfoRunLogInfoL, ok
	}
	tmp, ok := log.(*RunLogInfo)
	if ok {
		logInfoRunLogInfoL = tmp
	}
	return logInfoRunLogInfoL, ok
}

type LogName string

func (l LogName) String() string {
	return string(l)
}

//RunLogInfo 运行是日志信息
type RunLogInfo struct {
	Context       context.Context `json:"context"`
	Name          string          `json:"name"`
	OriginalInput string          `json:"originalInput"`
	DefaultJson   string          `json:"defaultJson"`
	PreInput      string          `json:"preInput"`
	PreOutput     string          `json:"preOutInput"`
	Out           string          `json:"out"`
	PostOut       interface{}     `json:"postOut"`
	Err           error
	logchan.EmptyLogInfo
}

func (l RunLogInfo) GetName() logchan.LogName {

	return LogName(l.Name)
}

func (l RunLogInfo) Error() error {
	return l.Err
}
