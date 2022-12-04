package datacenter

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/module/db"
)

type SourceInterface interface {
	Exec(s string) (string, error) // label 用来坐标记，方便分类处理
	GetSource() (handler interface{})
}
type SourcePool struct {
	tengo.Object
	Value map[string]SourceInterface
	lock  sync.Mutex
}

func (s *SourcePool) RegisterSource(identifier string, source SourceInterface) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Value[identifier] = source
	return nil
}

func (s *SourcePool) GetSource(identifier string) (source SourceInterface, err error) {
	source, ok := s.Value[identifier]
	if !ok {
		err = errors.Errorf("not found identifier(%s) source", identifier)
		return nil, err
	}
	return source, nil
}

func (s *SourcePool) TypeName() string {
	return "sourcePool"
}
func (s *SourcePool) String() string {
	ids := make([]string, 0)
	for id := range s.Value {
		ids = append(ids, id)
	}
	return strings.Join(ids, ",")
}

func (s *SourcePool) Call(args ...tengo.Object) (outObj tengo.Object, err error) {
	argLen := len(args)
	if argLen != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	var identifier string
	var data string
	var ok bool

	identifier, ok = tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "SourcePoll.args[0](identifier)",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	data, ok = tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "SourcePoll.args[0](tpl)",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	source, err := s.GetSource(identifier)
	if err != nil {
		return nil, err
	}
	out, err := source.Exec(data)
	if err != nil {
		return nil, err
	}
	outObj = &tengo.String{Value: out}
	return outObj, nil
}

const (
	PROVIDER_SQL      = "SQL"
	PROVIDER_CURL     = "CURL"
	PROVIDER_BIN      = "BIN"
	PROVIDER_REDIS    = "REDIS"
	PROVIDER_RABBITMQ = "RABBITMQ"
)

// MakeExecProvider 根据名称，获取exec 执行器，后续改成注册执行器方式
func MakeSource(identifier string, configJson string) (execProvider SourceInterface, err error) {

	switch identifier {
	case PROVIDER_SQL:
		var config db.DBExecProviderConfig
		if configJson != "" {
			err = json.Unmarshal([]byte(configJson), &config)
			if err != nil {
				return nil, err
			}
		}
		execProvider = &db.DBExecProvider{
			Config: config,
		}
	default:
		err = errors.Errorf("not suport source type :%s", identifier)
		return nil, err
	}
	return execProvider, nil
}
