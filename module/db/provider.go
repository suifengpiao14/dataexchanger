package db

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/template"
	"github.com/suifengpiao14/datacenter/util"
)

var DriverName = "mysql"

const (
	SQL_TYPE_SELECT = "SELECT"
	SQL_TYPE_OTHER  = "OTHER"
	LOG_LEVEL_DEBUG = "debug"
	LOG_LEVEL_INFO  = "info"
	LOG_LEVEL_ERROR = "error"
)

type DBExecProviderConfig struct {
	DSN      string `json:"dsn"`
	LogLevel string `json:"logLevel"`
	Timeout  int    `json:"timeout"`
}

type DBExecProvider struct {
	Config DBExecProviderConfig
	db     *sql.DB
	dbOnce sync.Once
}

func (p *DBExecProvider) Exec(s string, data interface{}) (interface{}, error) {
	return dbProvider(p, s, data)
}

func (p *DBExecProvider) GetSource() (source interface{}) {
	return p.db
}

// GetDb is a signal DB
func (p *DBExecProvider) GetDb() *sql.DB {
	if p.db == nil {
		if p.Config.DSN == "" {
			err := errors.Errorf("DBExecProvider %#v DNS is null ", p)
			panic(err)
		}
		p.dbOnce.Do(func() {
			db, err := sql.Open(DriverName, p.Config.DSN)
			if err != nil {
				panic(err)
			}
			p.db = db

		})
	}
	return p.db
}

//SQLType 判断 sql  属于那种类型
func SQLType(sqls string) string {
	sqlArr := strings.Split(sqls, template.EOF)
	selectLen := len(SQL_TYPE_SELECT)
	for _, sql := range sqlArr {
		if len(sql) < selectLen {
			continue
		}
		pre := sql[:selectLen]
		if strings.ToUpper(pre) == SQL_TYPE_SELECT {
			return SQL_TYPE_SELECT
		}
	}
	return SQL_TYPE_OTHER
}

func dbProvider(p *DBExecProvider, sqls string, data interface{}) (out interface{}, err error) {
	sqls = util.StandardizeSpaces(util.TrimSpaces(sqls)) // 格式化sql语句
	sqlType := SQLType(sqls)
	db := p.GetDb()
	var args []interface{}
	if data != nil { // todo 兼容数组
		rv := reflect.Indirect(reflect.ValueOf(data))
		rvK := rv.Kind()
		if rvK == reflect.Array || rvK == reflect.Slice {
			var ok bool
			args, ok = data.([]interface{})
			if !ok {
				args = *(data.(*[]interface{}))
			}
		} else {
			sqls, args, err = sqlx.Named(sqls, data)
			if err != nil {
				err = errors.WithStack(err)
				return "", err
			}
		}
	}

	if sqlType != SQL_TYPE_SELECT {
		res, err := db.Exec(sqls, args...)
		if err != nil {
			return "", err
		}
		lastInsertId, _ := res.LastInsertId()
		if lastInsertId > 0 {
			return strconv.FormatInt(lastInsertId, 10), nil
		}
		rowsAffected, _ := res.RowsAffected()
		return strconv.FormatInt(rowsAffected, 10), nil
	}
	rows, err := db.Query(sqls, args...)
	if err != nil {
		return "", err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}()
	allResult := make([][]map[string]interface{}, 0)
	for {
		records := make([]map[string]interface{}, 0)
		for rows.Next() {
			var record = make(map[string]interface{})

			err := MapScan(*rows, record)
			if err != nil {
				return "", err
			}
			records = append(records, record)
		}
		allResult = append(allResult, records)
		if !rows.NextResultSet() {
			break
		}
	}

	if len(allResult) == 1 { // allResult 初始值为[[]],至少有一个元素
		result := allResult[0]
		if len(result) == 0 { // 结果为空，返回空字符串
			return "", nil
		}
		if len(result) == 1 && len(result[0]) == 1 {
			row := result[0]
			for _, val := range row {
				return val, nil // 只有一个值时，直接返回值本身
			}
		}
		return result, nil
	}
	return allResult, nil
}

//MapScan copy sqlx
func MapScan(r sql.Rows, dest map[string]interface{}) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	return r.Err()
}
