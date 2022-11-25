package db

import (
	"github.com/d5/tengo/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type ExecSQL struct {
	tengo.ObjectImpl
	provider *DBExecProvider
}

func NewEXECSQL(config DBExecProviderConfig) *ExecSQL {
	return &ExecSQL{
		provider: &DBExecProvider{
			Config: config,
		},
	}
}

func (db *ExecSQL) TypeName() string {
	return "exec-sql"
}
func (db *ExecSQL) String() string {
	return ""
}

func (db *ExecSQL) Call(args ...tengo.Object) (tplOut tengo.Object, err error) {
	argLen := len(args)
	if argLen != 1 && argLen != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	sqlObj := args[0]
	sql, ok := tengo.ToString(sqlObj)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "sql",
			Expected: "string",
			Found:    sqlObj.TypeName(),
		}
	}

	var data interface{}
	if argLen == 2 {
		data = tengo.ToInterface(args[1])
	}
	out, err := db.provider.Exec(sql, data)
	if err != nil {
		return tengo.UndefinedValue, err
	}
	switch records := out.(type) {
	case []map[string]interface{}:
		arrMap := &tengo.Array{
			Value: make([]tengo.Object, 0),
		}
		for _, record := range records {
			mapStr, _ := tengo.FromInterface(record)
			arrMap.Value = append(arrMap.Value, mapStr)
		}
		return arrMap, nil
	default:
		tplOut, err = tengo.FromInterface(out)
		if err != nil {
			err = errors.WithMessage(err, "get db result")
			return nil, err
		}
	}
	return tplOut, nil
}
