package sqltpl

import (
	"github.com/d5/tengo/v2"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/template"
	gormLogger "gorm.io/gorm/logger"
)

func NewSQLTemplate() *template.Template {
	return template.NewTemplate(TemplatefuncMap)
}

func SQLTemplateOut2SQL(args ...tengo.Object) (sqlObj tengo.Object, err error) {
	if len(args) != 1 {
		return nil, tengo.ErrWrongNumArguments
	}
	namedSQLObj, ok := args[0].(*template.TemplateOut)
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
	sqlStr := gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
	sqlObj = &tengo.String{Value: sqlStr}
	return sqlObj, nil
}
