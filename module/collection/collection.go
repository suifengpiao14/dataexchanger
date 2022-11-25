package collection

import (
	"github.com/d5/tengo/v2"
	"github.com/jianfengye/collection"
	"github.com/pkg/errors"
)

type CollectionObj struct {
	tengo.ImmutableMap
	iCollection collection.ICollection
	value       map[string]*tengo.UserFunction
}

// TypeName returns the name of the type.
func (c *CollectionObj) TypeName() string {
	return "collection"
}

func (c *CollectionObj) CanCall() bool {
	return true
}

func (o *CollectionObj) Call(args ...tengo.Object) (ret tengo.Object, err error) {
	return nil, nil
}

func NewCollection(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}
	switch args[0].TypeName() {
	case "int":
		arr := make([]int, 0)
		for i, arg := range args {
			intV, ok := tengo.ToInt(arg)
			if !ok {
				err = errors.Errorf("the %d arg is not int", i)
				return nil, err
			}
			arr = append(arr, intV)
		}

		collectionObj := &CollectionObj{
			iCollection: collection.NewIntCollection(arr),
			value:       map[string]*tengo.UserFunction{},
		}

		collectionObj.value["groupBy"] = &tengo.UserFunction{Value: GroupBy(collectionObj)}

		return collectionObj, nil
	case "string":
		arr := make([]int, 0)
		for i, arg := range args {
			intV, ok := tengo.ToInt(arg)
			if !ok {
				err = errors.Errorf("the %d arg is nut int", i)
				return nil, err
			}
			arr = append(arr, intV)
		}
		collection.NewIntCollection(arr)
	default:
		err = errors.Errorf("collection not sourport type:%s", args[0].TypeName())
		return nil, err

	}
	err = errors.Errorf("collection not sourport type:%s", args[0].TypeName())
	return nil, err
}

func GroupBy(collectionObj *CollectionObj) tengo.CallableFunc {
	return func(args ...tengo.Object) (ret tengo.Object, err error) {
		if len(args) != 1 {
			return nil, tengo.ErrWrongNumArguments
		}
		fn, ok := args[0].(*tengo.UserFunction)
		if !ok {
			err = tengo.ErrInvalidArgumentType{
				Name:     "NewCollection th 1 arg",
				Expected: "userFunction",
				Found:    args[0].TypeName(),
			}
			return nil, err
		}

		collectionObj.iCollection.GroupBy(func(i1 interface{}, i2 int) interface{} {
			interfaceArg, err := tengo.FromInterface(i1)
			if err != nil {
				return err
			}
			index := &tengo.Int{Value: int64(i2)}
			result, err := fn.Value(interfaceArg, index)
			if err != nil {
				return err
			}
			return tengo.ToInterface(result)
		})

		return
	}
}
