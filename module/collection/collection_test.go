package collection

import (
	"fmt"
	"testing"

	"github.com/d5/tengo/v2"
)

func TestNewCollection(t *testing.T) {

	script := tengo.NewScript([]byte(`
	arr:=[1,1,2,3,4,4]
	coll:=newCollection(arr...)
	
	groupBy:=coll.groupBy(func(item ,index ){
		foo := int(item)
		return foo
	})
`))
	err := script.Add("newCollection", NewCollection)
	if err != nil {
		panic(err)
	}
	compiled, err := script.Run()
	if err != nil {
		panic(err)
	}
	v := compiled.Get("groupBy")
	fmt.Println(v)

}
