package db

import (
	"github.com/d5/tengo/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	gormLogger "gorm.io/gorm/logger"
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

	if argLen == 2 {
		data := tengo.ToInterface(args[1])
		statment, arguments, err := sqlx.Named(sql, data)
		if err != nil {
			err = errors.WithStack(err)
			return nil, err
		}
		sql = gormLogger.ExplainSQL(statment, nil, `'`, arguments...)

	}
	out, err := db.provider.Exec(sql)
	if err != nil {
		return tengo.UndefinedValue, err
	}
	tplOut = &tengo.String{Value: out}
	return tplOut, nil
}
