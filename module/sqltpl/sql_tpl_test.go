package sqltpl

import (
	"context"
	"fmt"
	"testing"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/stretchr/testify/require"
)

func TestNewNamedStatment(t *testing.T) {
	s := "{{define \"GetQuestionByID\"}} select * from `t_eva_item_all` where  `Fid`=:QID  {{if .ClassID}} and  `Fitem_class_id` in ({{in . .ClassID}}) {{end}} and `Fvalid`=1 and `Fcheck_item`=1  order by Fid asc; {{end}}"
	sqlTemplate := NewSQLTemplate()
	sqlTemplate.AddTpl("", s)
	name := &tengo.String{
		Value: "GetQuestionByID",
	}
	data, err := tengo.FromInterface(map[string]interface{}{
		"QID":     "1",
		"ClassID": []interface{}{"1", "2"},
	})
	if err != nil {
		panic(err)
	}
	ret, err := sqlTemplate.Call(name, data)
	require.NoError(t, err)
	fmt.Println(ret)
}

func TestExecSQL(t *testing.T) {
	tpl := "{{define \"GetQuestionByID\"}} select * from `t_eva_item_all` where  collector(`Fid`=:QID) `Fid`=:QID  {{if .ClassID}} and  `Fitem_class_id` in ({{in . .ClassID}}) {{end}} and `Fvalid`=1 and `Fcheck_item`=1  order by Fid asc; {{end}}"
	script := tengo.NewScript([]byte(`
	input["QID"]="3"
	sqlTplOut:=sqlTemplate("GetQuestionByID",input)
	output:=sqlTemplateOut2SQL(sqlTplOut)
`))
	data, err := tengo.FromInterface(map[string]interface{}{
		"QID":     "1",
		"ClassID": []interface{}{"1", "2"},
	})
	if err != nil {
		panic(err)
	}

	sqlTemplate := NewSQLTemplate()
	sqlTemplate.AddTpl("", tpl)
	sqlTemplate.AddTpl("", `{{define "a"}} hello world{{end}}`)
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	script.Add("sqlTemplate", sqlTemplate)
	script.Add("sqlTemplateOut2SQL", SQLTemplateOut2SQL)
	script.Add("input", &tengo.Map{})
	scriptCompiled, err := script.Compile()
	if err != nil {
		panic(err)
	}

	err = scriptCompiled.Set("input", data)
	if err != nil {
		panic(err)
	}

	err = scriptCompiled.RunContext(context.Background())
	if err != nil {
		panic(err)
	}
	v := scriptCompiled.Get("output")
	fmt.Println(v.String())
}
