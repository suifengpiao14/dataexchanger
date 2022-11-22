package sql

import (
	"encoding/json"
	"reflect"
	"strings"
	"text/template"

	"bytes"

	"github.com/Masterminds/sprig/v3"
	"github.com/d5/tengo/v2"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
	gormLogger "gorm.io/gorm/logger"
)

const (
	EOF                  = "\n"
	WINDOW_EOF           = "\r\n"
	HTTP_HEAD_BODY_DELIM = EOF + EOF
)

type NamedSQL struct {
	tengo.ObjectImpl
	NamedSQL string                 `json:"namedSQL"`
	Data     map[string]interface{} `json:"data"`
}

func (t *NamedSQL) TypeName() string {
	return "named-sql"
}
func (t *NamedSQL) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

type NamedStatment struct {
	tengo.ObjectImpl
	template *template.Template
	tpl      string
}

func NewNamedStatment(s string) *NamedStatment {
	tpl := NewTemplateByStr("", s)
	return &NamedStatment{
		template: tpl,
		tpl:      s,
	}
}

func (t *NamedStatment) TypeName() string {
	return "named-statment"
}
func (t *NamedStatment) String() string {
	return t.tpl
}
func (t *NamedStatment) CanCall() bool {
	return true
}

func (t *NamedStatment) Call(args ...tengo.Object) (namedStatment tengo.Object, err error) {
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

	namedSQL, namedData, err := t.ToNamedSQL(tplName, volume)
	if err != nil {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "data",
			Expected: "map",
			Found:    err.Error(),
		}
	}
	namedSQLObj := &NamedSQL{
		NamedSQL: namedSQL,
		Data:     namedData,
	}
	return namedSQLObj, nil
}

func (t *NamedStatment) ToNamedSQL(name string, volume VolumeInterface) (namedSQL string, namedData map[string]interface{}, err error) {

	var b bytes.Buffer
	err = t.template.ExecuteTemplate(&b, name, volume)
	if err != nil {
		err = errors.WithStack(err)
		return namedSQL, namedData, err
	}
	out := strings.ReplaceAll(b.String(), WINDOW_EOF, EOF)
	out = util.TrimSpaces(out)
	namedSQL = util.StandardizeSpaces(out)
	if namedSQL == "" {
		err = errors.Errorf("sql template :%s return empty sql", name)
		panic(err)
	}
	namedData, err = getNamedData(volume)
	if err != nil {
		err = errors.WithMessagef(err, "getNamedData")
		return namedSQL, namedData, err
	}
	return namedSQL, namedData, nil
}

func getNamedData(data interface{}) (out map[string]interface{}, err error) {
	out = make(map[string]interface{})
	if data == nil {
		return
	}
	dataI, ok := data.(*interface{})
	if ok {
		data = *dataI
	}
	mapOut, ok := data.(map[string]interface{})
	if ok {
		out = mapOut
		return
	}
	mapOutRef, ok := data.(*map[string]interface{})
	if ok {
		out = *mapOutRef
		return
	}
	if mapOut, ok := data.(volumeMap); ok {
		out = mapOut
		return
	}
	if mapOutRef, ok := data.(*volumeMap); ok {
		out = *mapOutRef
		return
	}

	v := reflect.Indirect(reflect.ValueOf(data))

	if v.Kind() != reflect.Struct {
		return
	}
	vt := v.Type()
	// 提取结构体field字段
	fieldNum := v.NumField()
	for i := 0; i < fieldNum; i++ {
		fv := v.Field(i)
		ft := fv.Type()
		fname := vt.Field(i).Name
		if fv.Kind() == reflect.Ptr {
			fv = fv.Elem()
			ft = fv.Type()
		}
		ftk := ft.Kind()
		switch ftk {
		case reflect.Int:
			out[fname] = fv.Int()
		case reflect.Int64:
			out[fname] = int64(fv.Int())
		case reflect.Float64:
			out[fname] = fv.Float()
		case reflect.String:
			out[fname] = fv.String()
		case reflect.Struct, reflect.Map:
			subOut, err := getNamedData(fv.Interface())
			if err != nil {
				return out, err
			}
			for k, v := range subOut {
				if _, ok := out[k]; !ok {
					out[k] = v
				}
			}

		default:
			out[fname] = fv.Interface()
		}
	}
	return
}

func NewTemplateByStr(name string, s string) (tmpl *template.Template) {
	tmpl = newTemplate(name)
	template.Must(tmpl.Parse(s))
	return tmpl
}

func newTemplate(name string) *template.Template {
	return template.New(name).Funcs(TemplatefuncMap).Funcs(sprig.TxtFuncMap())
}

func NamedSQL2SQL(args ...tengo.Object) (sqlObj tengo.Object, err error) {

	if len(args) != 1 {
		return nil, tengo.ErrWrongNumArguments
	}
	namedSQLObj, ok := args[0].(*NamedSQL)
	namedSQLInstance := NamedSQL{}
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "NamedSQL",
			Expected: namedSQLInstance.TypeName(),
			Found:    args[1].TypeName(),
		}
	}
	statment, arguments, err := sqlx.Named(namedSQLObj.NamedSQL, namedSQLObj.Data)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	sqlStr := gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
	sqlObj = &tengo.String{Value: sqlStr}
	return sqlObj, nil
}
