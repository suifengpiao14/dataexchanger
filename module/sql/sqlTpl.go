package sql

import (
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/d5/tengo/v2"
)

type NamedStatment struct {
	SQL  string
	Data map[string]interface{}
}

type SQLTpl struct {
	tengo.ObjectImpl
	template      *template.Template
	tpl           string
	namedStatment NamedStatment
	sql           string
}

func NewSQLTpl(s string) *SQLTpl {
	tpl := NewTemplateByStr("", s)
	return &SQLTpl{
		template:      tpl,
		tpl:           s,
		namedStatment: NamedStatment{},
	}
}

func (t *SQLTpl) String() string {
	return t.sql
}

func (t *SQLTpl) CanCall() bool {
	return true
}

func (t *SQLTpl) Call(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	fn, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "functionName",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	data, ok := args[1].(*tengo.Map)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    args[1].TypeName(),
		}
	}
	switch fn {
	case "sql":
		sql := t.ToSQL(data)
		ret = tengo.String{
			Value: sql,
		}
		return ret, nil
	default:
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "functionName",
			Expected: "sql",
			Found:    args[0].TypeName(),
		}
	}

	return tengo.UndefinedValue, nil
}

func (t *SQLTpl) ToSQL(data map[string]interface{}) (sql string) {
	return sql
}

func NewTemplateByStr(name string, s string) (tmpl *template.Template) {
	tmpl = newTemplate(name)
	template.Must(tmpl.Parse(s))
	return tmpl
}

func newTemplate(name string) *template.Template {
	return template.New(name).Funcs(TemplatefuncMap).Funcs(sprig.TxtFuncMap())
}
