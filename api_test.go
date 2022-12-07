package datacenter

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/suifengpiao14/datacenter/module/db"
	"github.com/suifengpiao14/datacenter/module/sqltpl"
	"github.com/suifengpiao14/datacenter/module/template"
)

func TestAPI(t *testing.T) {
	api := &API{
		Methods: "post",
		Route:   "/api/1/hello",
		InputLineSchema: `version=http://json-schema.org/draft-07/schema,id=input,direction=in
		fullname=pageIndex,dst=pageIndex,format=number,required
		fullname=pageSize,dst=pageSize,format=number,required`,
		OutputLineSchema: `version=http://json-schema.org/draft-07/schema,id=output,direction=out
		fullname=items[].content,src=PaginateOut.#.content,required
		fullname=items[].createdAt,src=PaginateOut.#.created_at,required
		fullname=items[].deletedAt,src=PaginateOut.#.deleted_at,required
		fullname=items[].description,src=PaginateOut.#.description,required
		fullname=items[].icon,src=PaginateOut.#.icon,required
		fullname=items[].id,src=PaginateOut.#.id,required
		fullname=items[].key,src=PaginateOut.#.key,required
		fullname=items[].label,src=PaginateOut.#.label,required
		fullname=items[].thumb,src=PaginateOut.#.thumb,required
		fullname=items[].title,src=PaginateOut.#.title,required
		fullname=items[].updatedAt,src=PaginateOut.#.updated_at,required
		fullname=pageInfo.pageIndex,src=input.pageIndex,required
		fullname=pageInfo.pageSize,src=input.pageSize,required
		fullname=pageInfo.total,src=PaginateTotalOut,required`,
		//{pageInfo:{pageIndex:input.pageIndex,pageSize:input.pageSize,total:PaginateTotalOut},items:{content:PaginateOut.#.content,createdAt:PaginateOut.#.created_at,deletedAt:PaginateOut.#.deleted_at}|@group}
		PreScript: `
		input["Offset"]=int(input["pageIndex"]) * int(input["pageSize"])
		input["Limit"]=int(input["pageSize"])
		`,
		MainScript: `
		
		PaginateTotalOut:=execSQLTPL("PaginateTotal",input)
		PaginateOut :=execSQLTPL("Paginate",input)
		out:=""
		out=GSjson.Set(out,"input",input)
		out=GSjson.SetRaw(out,"PaginateTotalOut",PaginateTotalOut)
		out=GSjson.SetRaw(out,"PaginateOut",PaginateOut)
		return  out
		`,
		PostScript: ``,
		AfterEvent: ``,
		Tpl: `
		`,
	}
	capi, err := NewApiCompiled(api)
	if err != nil {
		panic(err)
	}
	tplStr := `
	{{define "PaginateWhere"}} {{end}}
	{{define "PaginateTotal"}} select count(*) as count from component where 1=1 {{template "PaginateWhere" .}} and deleted_at is null; {{end}}
	{{define "Paginate"}} select * from component where 1=1 {{template "PaginateWhere" .}} and deleted_at is null order by updated_at desc limit :Offset,:Limit ; {{end}}
	`
	sourceConfig := `
	{"logLevel":"debug","dsn":"root:123456@tcp(10.0.11.125:3306)/office_web_site?charset=utf8&timeout=1s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true","timeout":30}
	`

	tpl := template.NewTemplate(sqltpl.TemplatefuncMap)
	tplNames := tpl.AddTpl("", tplStr)
	capi.WithTemplate(tpl.Template)
	var dbConfig db.DBExecProviderConfig
	err = json.Unmarshal([]byte(sourceConfig), &dbConfig)
	if err != nil {
		panic(err)
	}
	source := db.DBExecProvider{Config: dbConfig}
	for _, tplName := range tplNames {
		capi.WithSource(tplName, &source)
	}

	inputJson := `{"pageIndex":"","pageSize":"20"}`
	out, err := capi.Run(inputJson)
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
}
