package gsjson

import (
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func Get(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	jsonStr, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg1",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	path, ok := tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg2",
			Expected: "string",
			Found:    args[1].TypeName(),
		}
	}
	result := gjson.Get(jsonStr, path)
	ret = &tengo.String{Value: result.String()}

	return ret, nil
}

func Set(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 3 {
		return nil, tengo.ErrWrongNumArguments
	}
	jsonStr, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg1",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	path, ok := tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg2",
			Expected: "string",
			Found:    args[1].TypeName(),
		}
	}
	value := tengo.ToInterface(args[2])
	str, err := sjson.Set(jsonStr, path, value)
	if err != nil {
		err = errors.WithMessage(err, "sjson.set")
		return nil, err
	}
	ret = &tengo.String{Value: str}

	return ret, nil
}
