package db

import (
	"context"

	"github.com/d5/tengo/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/tengocontext"
	gormLogger "gorm.io/gorm/logger"
)

type ExecSQL struct {
	tengo.ObjectImpl
	provider *DBExecProvider
}
type SQLLogInfo struct {
	Context context.Context
	Name    string      `json:"name"`
	SQL     string      `json:"sql"`
	Named   string      `json:"named"`
	Data    interface{} `json:"data"`
	Result  string      `json:"result"`
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
	if argLen != 2 && argLen != 3 {
		return nil, tengo.ErrWrongNumArguments
	}

	ctxObj := args[0]
	ctx, ok := ctxObj.(*tengocontext.TengoContext)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "context",
			Expected: "context.Context",
			Found:    ctxObj.TypeName(),
		}
	}
	sqlLogInfo := SQLLogInfo{
		Context: ctx.Context,
		Name:    "ExecSQL",
	}
	sqlObj := args[1]
	sql, ok := tengo.ToString(sqlObj)
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "sql",
			Expected: "string",
			Found:    sqlObj.TypeName(),
		}
	}

	if argLen == 3 {
		data := tengo.ToInterface(args[2])
		statment, arguments, err := sqlx.Named(sql, data)
		if err != nil {
			err = errors.WithStack(err)
			return nil, err
		}
		sql = gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
		sqlLogInfo.Data = data
		sqlLogInfo.Named = statment
	}
	sqlLogInfo.SQL = sql
	out, err := db.provider.Exec(sql)
	if err != nil {
		return tengo.UndefinedValue, err
	}
	sqlLogInfo.Result = out
	tplOut = &tengo.String{Value: out}
	return tplOut, nil
}
