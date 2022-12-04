package gsjson

import (
	"encoding/json"
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

// unwrap removes the '[]' or '{}' characters around json
func unwrap(json string) string {
	json = trim(json)
	if len(json) >= 2 && (json[0] == '[' || json[0] == '{') {
		json = json[1 : len(json)-1]
	}
	return json
}

func trim(s string) string {
left:
	if len(s) > 0 && s[0] <= ' ' {
		s = s[1:]
		goto left
	}
right:
	if len(s) > 0 && s[len(s)-1] <= ' ' {
		s = s[:len(s)-1]
		goto right
	}
	return s
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
	gjson.AddModifier("tonum", tonum) // 列转换为数字,可用于字符串数字排序
	gjson.AddModifier("combine", combine)

	gjson.AddModifier("leftJoin", leftJoin)
	gjson.AddModifier("index", index)
	gjson.AddModifier("concat", concat) //将二维数组按行合并成一维数组,可用于多列索引
}

var GSjon = map[string]tengo.Object{
	"Get": &tengo.UserFunction{
		Value: Get,
	},
	"Set": &tengo.UserFunction{
		Value: Set,
	},
	"SetRaw": &tengo.UserFunction{
		Value: SetRaw,
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
func SetRaw(args ...tengo.Object) (ret tengo.Object, err error) {
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
	value, ok := tengo.ToString(args[2])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "gjson.get.arg3",
			Expected: "string",
			Found:    args[2].TypeName(),
		}
	}
	str, err := sjson.SetRaw(jsonStr, path, value)
	if err != nil {
		err = errors.WithMessage(err, "sjson.setRaw")
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
	var kvMap = map[string]struct{}{}
	for k, v := range keys {
		key := v.String()
		if _, ok := kvMap[key]; ok {
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
		kvMap[key] = struct{}{}
	}
	out = append(out, '}')
	return bytesString(out)
}

func leftJoin(jsonStr, arg string) string {
	if arg == "" {
		return jsonStr
	}
	if arg[0] != '[' && arg[0] != '{' {
		arg = fmt.Sprintf("[%s]", arg)
	}
	var container = jsonStr
	var sels []subSelector
	var selsLen = 0
	var err error
	ok := false
	sels, _, ok = parseSubSelectors(arg)
	if !ok {
		return container
	}
	selsLen = len(sels)
	if selsLen < 2 {
		return jsonStr
	}

	if selsLen%2 != 0 {
		err = errors.Errorf("leftJoin:The path contained in the parameter must be an even number,got:%s", arg)
		panic(err)
	}
	for i := len(sels) - 1; i >= 0; i = i - 2 {
		left := sels[i-1]
		right := sels[i]
		sub := leftJoin2Path(container, left.path, right.path)
		leftRowGetPath := getParentPath(left.path)
		leftRowSetPath := nameOfLast(leftRowGetPath) // 获取数组下标
		container, err = sjson.SetRaw(container, leftRowSetPath, sub)
		if err != nil {
			err = errors.WithMessage(err, "leftJoin:")
			panic(err)
		}
	}
	out := gjson.Get(container, "@this.0").String() // 返回数组的第一个
	return out

}

func leftJoin2Path(jsonStr, leftPath, rightPath string) string { //合并2个元素
	res := gjson.Parse(jsonStr)
	firstRowPath := getParentPath(leftPath)
	secondRowPath := getParentPath(rightPath)
	firstRef := fmt.Sprintf("[[%s]|@concat,%s]", unwrap(leftPath), firstRowPath)
	firstRefArr := res.Get(firstRef).Array()
	indexArr, rowArr := firstRefArr[0].Array(), firstRefArr[1].Array()
	secondMapPath := fmt.Sprintf("[[%s]|@concat,%s]|@combine", unwrap(rightPath), secondRowPath)
	secondMap := res.Get(secondMapPath).Map()
	secondDefault := map[string]gjson.Result{}
	for _, v := range secondMap {
		for key, value := range v.Map() {
			raw := `""`
			if value.Type == gjson.Number {
				raw = `0`
			}
			secondDefault[key] = gjson.Result{Type: value.Type, Str: `""`, Num: 0, Raw: raw}
		}
		break
	}

	var out []byte
	out = append(out, '[')

	for i, index := range indexArr {
		if i > 0 {
			out = append(out, ',')
		}
		row := rowArr[i].Map()
		secondV, ok := secondMap[index.String()]
		secondVMap := secondDefault
		if ok {
			secondVMap = secondV.Map()
		}
		for k, v := range secondVMap {
			if _, ok := row[k]; ok {
				k = fmt.Sprintf("%s1", k)
			}
			row[k] = v
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

func index(jsonStr, arg string) string {
	if arg == "" {
		return jsonStr
	}
	res := gjson.Parse(jsonStr)
	if arg[0] != '[' && arg[0] != '{' {
		arg = fmt.Sprintf(`[%s]`, arg) // 统一使用复合索引方式处理
	}
	rowPath := getParentPath(arg)
	refPath := fmt.Sprintf("[%s|@values,%s]", arg, rowPath)
	refArr := res.Get(refPath).Array()
	keyArr, rowArr := refArr[0].Array(), refArr[1].Array()
	indexMapArr := make(map[string][]gjson.Result)
	indexArr := concatColumn("-", keyArr...)
	for i, indexKey := range indexArr {
		if _, ok := indexMapArr[indexKey]; !ok {
			indexMapArr[indexKey] = make([]gjson.Result, 0)
		}
		indexMapArr[indexKey] = append(indexMapArr[indexKey], rowArr[i])
	}

	var out []byte
	out = append(out, '{')
	i := 0
	for k, arr := range indexMapArr {
		if i > 0 {
			out = append(out, ',')
		}
		i++
		out = append(out, fmt.Sprintf(`"%s"`, k)...)
		out = append(out, ':')
		out = append(out, '[')
		for j, row := range arr {
			if j > 0 {
				out = append(out, ',')
			}
			out = append(out, row.Raw...)
		}
		out = append(out, ']')
	}
	out = append(out, '}')
	outStr := bytesString(out)
	return outStr
}

func tonum(json string, arg string) (num string) {

	num = strings.Trim(json, `'"`)
	return num
}

func orderBy(json string, arg string) (num string) {
	num = strings.Trim(json, `'"`)
	return num
}

func concat(jsonStr, arg string) string {
	resArr := gjson.Parse(jsonStr).Array()
	sep := "-"
	if arg != "" {
		sep = arg
	}
	arr := concatColumn(sep, resArr...)
	b, err := json.Marshal(arr)
	if err != nil {
		err = errors.WithMessage(err, "gsjson.concat")
		panic(err)
	}
	out := string(b)
	return out

}

// concatColumn 合并一行中的所有数据，复合索引使用
func concatColumn(sep string, columns ...gjson.Result) (out []string) {
	out = make([]string, 0)
	clen := len(columns)
	if clen == 0 {
		return out
	}
	rlen := len(columns[0].Array())
	for i := 0; i < rlen; i++ {
		row := make([]string, 0)
		for j := 0; j < clen; j++ {
			column := columns[j].Array()
			row = append(row, column[i].String())
		}
		out = append(out, strings.Join(row, sep))
	}
	return out
}

func getParentPath(path string) string {
	path = util.TrimSpaces(path)
	if path[0] == '[' || path[0] == '{' {
		subs, newPath, ok := parseSubSelectors(path)
		if !ok {
			return path
		}
		if len(subs) == 0 {
			return newPath // todo 验证返回内容
		}
		path = subs[0].path // 取第一个路径计算父路径
	}
	path = nameOfPrefix(path)
	path = strings.Trim(path, ".#")
	if path == "" {
		path = "@this"
	}
	return path
}

// nameOfLast returns the name of the last component
func nameOfLast(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '|' || path[i] == '.' {
			if i > 0 {
				if path[i-1] == '\\' {
					continue
				}
			}
			return path[i+1:]
		}
	}
	return path
}

// nameOfPrefix returns the name of the path except the last component
func nameOfPrefix(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '|' || path[i] == '.' {
			if i > 0 {
				if path[i-1] == '\\' {
					continue
				}
			}
			return path[:i+1]
		}
	}
	return path
}

type subSelector struct {
	name string
	path string
}

// copy from gjson
// parseSubSelectors returns the subselectors belonging to a '[path1,path2]' or
// '{"field1":path1,"field2":path2}' type subSelection. It's expected that the
// first character in path is either '[' or '{', and has already been checked
// prior to calling this function.
func parseSubSelectors(path string) (sels []subSelector, out string, ok bool) {
	modifier := 0
	depth := 1
	colon := 0
	start := 1
	i := 1
	pushSel := func() {
		var sel subSelector
		if colon == 0 {
			sel.path = path[start:i]
		} else {
			sel.name = path[start:colon]
			sel.path = path[colon+1 : i]
		}
		sels = append(sels, sel)
		colon = 0
		modifier = 0
		start = i + 1
	}
	for ; i < len(path); i++ {
		switch path[i] {
		case '\\':
			i++
		case '@':
			if modifier == 0 && i > 0 && (path[i-1] == '.' || path[i-1] == '|') {
				modifier = i
			}
		case ':':
			if modifier == 0 && colon == 0 && depth == 1 {
				colon = i
			}
		case ',':
			if depth == 1 {
				pushSel()
			}
		case '"':
			i++
		loop:
			for ; i < len(path); i++ {
				switch path[i] {
				case '\\':
					i++
				case '"':
					break loop
				}
			}
		case '[', '(', '{':
			depth++
		case ']', ')', '}':
			depth--
			if depth == 0 {
				pushSel()
				path = path[i+1:]
				return sels, path, true
			}
		}
	}
	return
}
