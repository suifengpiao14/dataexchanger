package sql

import (
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/d5/tengo/v2"
)

const (
	EOF                  = "\n"
	WINDOW_EOF           = "\r\n"
	HTTP_HEAD_BODY_DELIM = EOF + EOF
)

type NamedStatment struct {
	tengo.ObjectImpl
	template *template.Template
	tpl      string
	SQL      string
	Data     map[string]interface{}
}

func NewSQLTpl(s string) *NamedStatment {
	tpl := NewTemplateByStr("", s)
	return &NamedStatment{
		template: tpl,
		tpl:      s,
	}
}

func (t *NamedStatment) String() string {
	return t.sql
}

func (t *NamedStatment) CanCall() bool {
	return true
}

func (t *NamedStatment) Call(args ...tengo.Object) (ret tengo.Object, err error) {
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
		ret = tengo.Map{
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

func (t *NamedStatment) ToSQL(name string, volume VolumeInterface) (namedSQL string, namedData map[string]interface{}, err error) {

	var b bytes.Buffer
	err = t.template.ExecuteTemplate(&b, name, volume)
	if err != nil {
		err = errors.WithStack(err)
		return namedSQL, namedData, err
	}
	out := strings.ReplaceAll(b.String(), WINDOW_EOF, EOF)
	out = util.TrimSpaces(out)

	tplOut := t.template.E(volume, templateName)
	tplOut = util.StandardizeSpaces(tplOut)
	if tplOut == "" {
		err := errors.Errorf("sql template :%s return empty sql", templateName)
		panic(err)
	}
	return namedSQL, namedData, nil
}

func NewTemplateByStr(name string, s string) (tmpl *template.Template) {
	tmpl = newTemplate(name)
	template.Must(tmpl.Parse(s))
	return tmpl
}

func newTemplate(name string) *template.Template {
	return template.New(name).Funcs(TemplatefuncMap).Funcs(sprig.TxtFuncMap())
}
