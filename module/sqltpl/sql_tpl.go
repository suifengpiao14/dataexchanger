package sqltpl

import (
	"github.com/suifengpiao14/datacenter/module/db"
	"github.com/suifengpiao14/datacenter/module/tengocontext"

	"github.com/d5/tengo/v2"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/logger"
	"github.com/suifengpiao14/datacenter/module/template"
	gormLogger "gorm.io/gorm/logger"
)

func NewSQLTemplate() *template.Template {
	return template.NewTemplate(TemplatefuncMap)
}

func SQLTemplateOut2SQL(args ...tengo.Object) (sqlObj tengo.Object, err error) {
	sqlLogInfo := db.SQLLogInfo{
		Name: db.SQL_LOG_INFO_EXEC,
	}
	defer func() {
		sqlLogInfo.Err = err
		logger.SendLogInfo(sqlLogInfo)
	}()
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	ctxObj, ok := args[0].(*tengocontext.TengoContext)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "context",
			Expected: "context.Context",
			Found:    args[0].TypeName(),
		}
	}
	sqlLogInfo.Context = ctxObj.Context

	namedSQLObj, ok := args[1].(*template.TemplateOut)
	namedSQLInstance := template.TemplateOut{}
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "NamedSQL",
			Expected: namedSQLInstance.TypeName(),
			Found:    args[1].TypeName(),
		}
	}
	statment, arguments, err := sqlx.Named(namedSQLObj.Out, namedSQLObj.Data)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	sqlLogInfo.Named = statment
	sqlLogInfo.Data = namedSQLObj.Data // 记录日志
	sqlStr := gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
	sqlObj = &tengo.String{Value: sqlStr}
	sqlLogInfo.SQL = sqlStr
	return sqlObj, nil
}
