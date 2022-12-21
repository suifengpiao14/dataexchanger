package sourcepool

import (
	"strings"
	"sync"

	"github.com/d5/tengo/v2"
	"github.com/go-errors/errors"
	"github.com/suifengpiao14/datacenter/source"
)

type SourcePool struct {
	tengo.Object
	Value map[string]source.SourceInterface
	lock  sync.Mutex
}

func (s *SourcePool) RegisterSource(identifier string, sourceProvider source.SourceInterface) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Value[identifier] = sourceProvider
	return nil
}

func (s *SourcePool) GetSource(identifier string) (sourceProvider source.SourceInterface, err error) {
	sourceProvider, ok := s.Value[identifier]
	if !ok {
		err = errors.Errorf("not found identifier(%s) source", identifier)
		return nil, err
	}
	return sourceProvider, nil
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
