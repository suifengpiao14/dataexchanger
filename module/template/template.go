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
	Template         *textTemplate.Template
	tpl              string
	sources          map[string]string // key 为 template name ,value 为资源ID
	templateFuncMaps []textTemplate.FuncMap
}

func NewTemplate(funcMap textTemplate.FuncMap) *Template {
	return &Template{
		Template:         textTemplate.New("").Funcs(funcMap).Funcs(sprig.TxtFuncMap()),
		templateFuncMaps: []textTemplate.FuncMap{funcMap, sprig.TxtFuncMap()},
	}
}

func (t *Template) AddTpl(name string, s string) (tplNames []string) {
	tmpl := t.Template.Lookup(name)
	if tmpl == nil {
		tmpl = t.Template.New(name)
	}
	textTemplate.Must(tmpl.Parse(s)) // 追加
	tmp := textTemplate.Must(t.newTemplate().Parse(s))
	tplNames = util.GetTemplateNames(tmp)

	t.tpl = fmt.Sprintf(`%s\n{{define "%s"}}%s{{end}}`, t.tpl, name, s)
	return tplNames
}
func (t *Template) newTemplate() *textTemplate.Template {
	tpl := textTemplate.New("").Funcs(sprig.TxtFuncMap())
	for _, funcMap := range t.templateFuncMaps {
		tpl = tpl.Funcs(funcMap)
	}
	return tpl
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
	volume := &VolumeMap{}
	for k, v := range tengoMap.Value {
		volume.SetValue(k, tengo.ToInterface(v))
	}
	var b bytes.Buffer
	err = t.Template.ExecuteTemplate(&b, tplName, volume)
	if err != nil {
		err = errors.WithStack(err)
		return tengo.UndefinedValue, err
	}
	out := strings.ReplaceAll(b.String(), WINDOW_EOF, EOF)
	out = util.TrimSpaces(out)
	tplOut = &TemplateOut{Out: out, Data: *volume}
	return tplOut, nil
}
