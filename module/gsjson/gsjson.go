package gsjson

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func bytesString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

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
	gjson.AddModifier("combine", combine)

	gjson.AddModifier("union", union)
}

var GSjon = map[string]tengo.Object{
	"Get": &tengo.UserFunction{
		Value: Get,
	},
	"Set": &tengo.UserFunction{
		Value: Set,
	},
	"GetSet": &tengo.UserFunction{
		Value: GetSet,
	},
	"Delete": &tengo.UserFunction{
		Value: Delete,
	},
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
	jsonStr = util.TrimSpaces(jsonStr)
	path = util.TrimSpaces(path)
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

func GetSet(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	jsonStr, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.GetSet.arg1",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	path, ok := tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.GetSet.arg2",
			Expected: "string",
			Found:    args[1].TypeName(),
		}
	}
	result := gjson.Parse(path).Array()
	var out = ""
	arrayKeys := map[string]struct{}{}
	for _, keyRow := range result {
		src := keyRow.Get("src").String()
		dst := keyRow.Get("dst").String()
		n1Index := strings.LastIndex(dst, "-1")
		if n1Index > -1 {
			parentKey := dst[:n1Index]
			parentKey = strings.TrimRight(parentKey, ".#")
			if parentKey[len(parentKey)-1] == ')' {
				parentKey = fmt.Sprintf("%s#", parentKey)
			}
			arrayKeys[parentKey] = struct{}{}
			dst = fmt.Sprintf("%s%s", dst[:n1Index-1], dst[n1Index+2:])
		}

		raw := gjson.Get(jsonStr, src).Raw
		out, err = sjson.SetRaw(out, dst, raw)
		if err != nil {
			err = errors.WithMessage(err, "gsjson.GetSet")
			return nil, err
		}
	}
	for path := range arrayKeys {
		qPath := fmt.Sprintf("%s|@group", path)
		raw := gjson.Get(out, qPath).Raw
		out, err = sjson.SetRaw(out, path, raw)
		if err != nil {
			err = errors.WithMessage(err, "gsjson.GetSet|@group")
			return nil, err
		}
	}

	ret = &tengo.String{Value: out}
	return ret, nil
}

func Delete(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}
	jsonStr, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.delete.arg1",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}
	path, ok := tengo.ToString(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.delete.arg2",
			Expected: "string",
			Found:    args[1].TypeName(),
		}
	}
	str, err := sjson.Delete(jsonStr, path)
	if err != nil {
		err = errors.WithMessage(err, "sjson.delete")
		return nil, err
	}
	ret = &tengo.String{Value: str}

	return ret, nil
}

func combine(jsonStr, arg string) string {
	res := gjson.Parse(jsonStr).Array()
	var out []byte
	out = append(out, '{')
	var keys []gjson.Result
	var values []gjson.Result
	for i, value := range res {
		switch i {
		case 0:
			keys = value.Array()
		case 1:
			values = value.Array()
		}
	}
	vlen := len(values)
	var kvMap = map[string]bool{}
	for k, v := range keys {
		key := v.String()
		if ok := kvMap[key]; ok {
			continue
		}
		out = append(out, v.Raw...)
		out = append(out, ':')
		if k < vlen {
			value := values[k]
			out = append(out, value.Raw...)
		} else {
			out = append(out, '"', '"')
		}
		kvMap[key] = true
	}
	out = append(out, '}')
	return bytesString(out)
}

func union(jsonStr, arg string) string {
	if arg == "" {
		return jsonStr
	}
	res := gjson.Parse(jsonStr)
	var firstPath string
	var secondPath string
	args := gjson.Parse(arg)
	firstPath = args.Get("firstPath").String()
	secondPath = args.Get("secondPath").String()

	firstRowPath := firstPath
	firstLastIndex := strings.LastIndex(firstRowPath, ".")
	if firstLastIndex > -1 {
		firstRowPath = strings.TrimRight(firstRowPath[:firstLastIndex], ".#")
		if firstRowPath[len(firstRowPath)-1] == ')' {
			firstRowPath = fmt.Sprintf("%s#", firstRowPath)
		}
	}
	firstMapPath := fmt.Sprintf("[%s,%s]|@combine", firstPath, firstRowPath)
	firstMap := res.Get(firstMapPath).Map()
	secondRowPath := secondPath
	secondLastIndex := strings.LastIndex(secondRowPath, ".")
	if secondLastIndex > -1 {
		secondRowPath = strings.TrimRight(secondRowPath[:secondLastIndex], ".#")
		if secondRowPath[len(secondRowPath)-1] == ')' {
			secondRowPath = fmt.Sprintf("%s#", secondRowPath)
		}
	}
	secondMapPath := fmt.Sprintf("[%s,%s]|@combine", secondPath, secondRowPath)
	secondMap := res.Get(secondMapPath).Map()
	var out []byte
	out = append(out, '[')
	i := 0
	for k, firstV := range firstMap {
		if i > 0 {
			out = append(out, ',')
		}
		i++
		row := firstV.Map()
		secondV, ok := secondMap[k]
		if ok {
			for k, v := range secondV.Map() {
				row[k] = v
			}
		}
		out = append(out, '{')
		j := 0
		for k, v := range row {
			if j > 0 {
				out = append(out, ',')
			}
			j++
			out = append(out, fmt.Sprintf(`"%s"`, k)...)
			out = append(out, ':')
			out = append(out, v.Raw...)

		}
		out = append(out, '}')
	}
	out = append(out, ']')
	outStr := bytesString(out)
	return outStr
}
