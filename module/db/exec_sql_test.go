package db

import (
	"fmt"
	"testing"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/require"
)

func TestExecSQL(t *testing.T) {

	cfg := DBExecProviderConfig{
		DSN:      "hjx:123456@tcp(recycle.m.mysql.hsb.com:3306)/recycle?charset=utf8&timeout=1s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true",
		LogLevel: "debug",
		Timeout:  30,
	}
	execSQL := NewEXECSQL(cfg)
	sqlObj := &tengo.String{
		Value: "select * from `t_eva_item_all` where   `Fitem_class_id` in (?,?)  and `Fvalid`=1 and `Fcheck_item`=1  order by Fid asc;",
	}
	dataObj, err := tengo.FromInterface([]interface{}{
		"1",
		"2",
	})
	if err != nil {
		panic(err)
	}
	ret, err := execSQL.Call(sqlObj, dataObj)
	require.NoError(t, err)
	fmt.Println(ret)
}
