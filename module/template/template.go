package template

import (
	"fmt"
	"strings"
	textTemplate "text/template"

	"bytes"

	"github.com/Masterminds/sprig/v3"
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
)

const (
	EOF                  = "\n"
	WINDOW_EOF           = "\r\n"
	HTTP_HEAD_BODY_DELIM = EOF + EOF
)

type TemplateOut struct {
	tengo.ObjectImpl
	Out  string                 `json:"out"`
	Data map[string]interface{} `json:"data"`
}

func (to *TemplateOut) TypeName() string {
	return "template-out"
}
func (to *TemplateOut) String() string {
	return to.Out
}

type Template struct {
	tengo.ObjectImpl
	template *textTemplate.Template
	tpl      string
}

func NewTemplate(funcMap textTemplate.FuncMap) *Template {
	return &Template{
		template: textTemplate.New("").Funcs(funcMap).Funcs(sprig.TxtFuncMap()),
	}
}

func (t *Template) AddTpl(name string, s string) (t1 *Template) {
	tmpl := t.template.Lookup(name)
	if tmpl == nil {
		tmpl = t.template.New(name)
	}
	textTemplate.Must(tmpl.Parse(s)) // 追加
	t.tpl = fmt.Sprintf(`%s\n{{define "%s"}}%s{{end}}`, t.tpl, name, s)
	return t
}

func (t *Template) TypeName() string {
	return "template"
}
func (t *Template) String() string {
	return t.tpl
}
func (t *Template) CanCall() bool {
	return true
}

func (t *Template) Call(args ...tengo.Object) (tplOut tengo.Object, err error) {
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}

	tplName, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "tplName",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	tengoMap, ok := args[1].(*tengo.Map)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    args[1].TypeName(),
		}
	}
	volume := &volumeMap{}
	for k, v := range tengoMap.Value {
		volume.SetValue(k, tengo.ToInterface(v))
	}
	var b bytes.Buffer
	err = t.template.ExecuteTemplate(&b, tplName, volume)
	if err != nil {
		err = errors.WithStack(err)
		return tengo.UndefinedValue, err
	}
	out := strings.ReplaceAll(b.String(), WINDOW_EOF, EOF)
	out = util.TrimSpaces(out)
	tplOut = &TemplateOut{Out: out, Data: *volume}
	return tplOut, nil
}
