package gsjson

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func init() {
	//支持大小写转换
	gjson.AddModifier("case", func(json, arg string) string {
		if arg == "upper" {
			return strings.ToUpper(json)
		}
		if arg == "lower" {
			return strings.ToLower(json)
		}
		return json
	})
	//支持二维数组转置
	gjson.AddModifier("transpose", func(jsonStr, arg string) string {
		out := ""
		isArr := jsonStr[0] == '['
		if isArr {
			out = row2Column(jsonStr)
		} else {
			out = column2Row(jsonStr)
		}
		return out
	})
}

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

// Merge 合并多个json
func Merge(args ...tengo.Object) (ret tengo.Object, err error) {
	argMapArr := make([]*tengo.Map, len(args))
	var ok bool
	for i, arg := range args {
		argMapArr[i], ok = arg.(*tengo.Map)
		if !ok {
			return nil, tengo.ErrInvalidArgumentType{
				Name:     fmt.Sprintf("gjson.Merge arg %d", i+1),
				Expected: "map",
				Found:    args[i].TypeName(),
			}
		}

	}
	var rentMap tengo.Map
	for i, argMap := range argMapArr {
		if i == 0 {
			rentMap = *argMap
			continue
		}
		for k, v := range argMap.Value {
			rentMap.Value[k] = v
		}

	}

	return &rentMap, err
}

// Merge 合并多个json
func Union(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 3 {
		return nil, tengo.ErrWrongNumArguments
	}
	first, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg1",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	second, ok := tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg2",
			Expected: "string",
			Found:    args[1].TypeName(),
		}
	}
	key, ok := tengo.ToString(args[2])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg3",
			Expected: "string",
			Found:    args[2].TypeName(),
		}
	}
	if first[0] != '[' || second[0] != '[' {
		err = errors.Errorf("first and second arg must be object of array")
		return nil, err
	}
	var firstArr []map[string]interface{}
	err = json.Unmarshal([]byte(first), &firstArr)
	if err != nil {
		err = errors.WithMessage(err, "gsjson.union.first arg")
		return nil, err
	}
	var secondArr []map[string]interface{}
	err = json.Unmarshal([]byte(first), &secondArr)
	if err != nil {
		err = errors.WithMessage(err, "gsjson.union.second arg")
		return nil, err
	}
	if len(firstArr) < 1 || len(secondArr) < 1 {
		err = errors.Errorf("first and second  object of arr argument len must big than 1")
		return nil, err
	}

	_, ok1 := firstArr[0][key]
	_, ok2 := secondArr[0][key]
	if !ok1 || !ok2 {
		err = errors.Errorf("first and second  object of arr argument requie key:%s", key)
		return nil, err
	}

	var secondMap = map[interface{}]map[string]interface{}{}
	for _, row := range secondArr {
		secondMap[row[key]] = row
	}
	var secondRowDefault = map[string]interface{}{}
	for k := range secondArr[0] {
		secondRowDefault[k] = nil
	}

	for index, row := range firstArr {
		secondRow, ok := secondMap[row[key]]
		if !ok {
			secondRow = secondRowDefault
		}
		for k, v := range secondRow {
			row[k] = v
		}
		firstArr[index] = row
	}
	b, err := json.Marshal(firstArr)
	if err != nil {
		err = errors.WithMessage(err, "gsjon.union")
		return nil, err
	}
	ret = &tengo.String{Value: string(b)}
	return ret, err
}

// column2Row 列数据(二维数组中一维为列对象，二维为值数组)转行数据(二维数组中，一维为行索引，二维为行对象) gjson 获取数据时，会将行数据，转换成列数据，此时需要调用该函数再转换为行数据
func column2Row(jsonStr string) (out string) {
	arr := make(map[string][]interface{}, 0)
	err := json.Unmarshal([]byte(jsonStr), &arr)
	if err != nil {
		panic(err)
	}
	var outArr []map[string]interface{}
	initOutArr := true
	for key, column := range arr {
		if initOutArr { //init array
			outArr = make([]map[string]interface{}, len(column))
			initOutArr = false
		}

		for i, val := range column {
			if outArr[i] == nil { //init map
				outArr[i] = make(map[string]interface{})
			}
			outArr[i][key] = val
		}
	}
	b, err := json.Marshal(outArr)
	if err != nil {
		panic(err)
	}
	out = string(b)
	return out
}

// row2Column 行数据(二维数组中，一维为行索引，二维为行对象)转 列数据(二维数组中一维为列对象，二维为值数组) ，有时json数据的key和结构体的json key（结构体由第三方包定义，不可修改tag） 不一致，但是存在对应关系，需要构造结构体内json key数据，此时将行数据改成列数据，方便计算
func row2Column(jsonStr string) (out string) {
	arr := make([]map[string]interface{}, 0)
	err := json.Unmarshal([]byte(jsonStr), &arr)
	if err != nil {
		panic(err)
	}
	outMap := make(map[string][]interface{}, 0)
	arrLen := len(arr)
	for i, row := range arr {
		for key, val := range row {
			if outMap[key] == nil {
				outMap[key] = make([]interface{}, arrLen)
			}
			outMap[key][i] = val
		}
	}
	b, err := json.Marshal(outMap)
	if err != nil {
		panic(err)
	}
	out = string(b)
	return out
}
