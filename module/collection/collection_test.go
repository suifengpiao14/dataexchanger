package collection

import (
	"fmt"
	"testing"

	"github.com/d5/tengo/v2"
)

func TestRecord_map(t *testing.T) {
	script := tengo.NewScript([]byte(`
	collection:=import("./collection")
	records:=[
		{"id":1,"name":"张三","age":20,"sex":"男"},
		{"id":2,"name":"李四","age":20,"sex":"男"},
		{"id":3,"name":"王五","age":18,"sex":"女"}
		]
		column:=collection.column(records,"sex","name")
		index:=collection.index(records,"id")
		map:=collection.map(records,func(record,i){
			record["status"]=1
			return record
		})
		group:=collection.group(records,"sex")
		keyConvert:=collection.keyConvert(records,{"id":"userId","name":"userName"})
		orderBy:=collection.orderBy(records,func(r1,r2){
			return r1.id>r2.id
		})
`))
	script.EnableFileImport(true)
	c, err := script.Run()
	if err != nil {
		panic(err)
	}
	v := c.Get("column")
	fmt.Println(v)
	v = c.Get("index")
	fmt.Println(v)
	v = c.Get("map")
	fmt.Println(v)
	v = c.Get("group")
	fmt.Println(v)
	v = c.Get("keyConvert")
	fmt.Println(v)
	v = c.Get("orderBy")
	fmt.Println(v)
}
