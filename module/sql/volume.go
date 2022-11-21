package sql

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

// 私有定义，确保对volumeMap 的操作全部通过 get/set 函数实现
type volumeMap map[string]interface{}

func (v *volumeMap) init() {
	if v == nil {
		err := errors.Errorf("*Templatemap must init")
		panic(err)
	}
	if *v == nil {
		*v = volumeMap{} // 解决 data33 情况
	}
}

func (v *volumeMap) SetValue(key string, value interface{}) {
	v.init()
	// todo 并发lock
	if !strings.Contains(key, ".") {
		(*v)[key] = value
		return
	}
	// 写入json
	firstDot := strings.Index(key, ".")
	root := key[:firstDot]
	jsonKey := key[firstDot+1:]
	data, ok := (*v)[root]
	if !ok {
		data = ""
	}
	dstType := "string"
	str, ok := data.(string)
	if !ok {
		b, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		str = string(b)
	}
	err := Add2json(&str, jsonKey, dstType, value)
	if err != nil {
		panic(err)
	}
	(*v)[root] = str
}

func (v *volumeMap) GetValue(key string, value interface{}) bool {
	v.init()

	tmp, ok := getValue(v, key)
	if !ok {
		return false
	}
	ok = convertType(value, tmp)
	return ok
}

func getValue(v *volumeMap, key string) (interface{}, bool) {
	var mapKey string
	var jsonKey string
	var value interface{}
	var ok bool
	mapKey = key
	for {
		value, ok = (*v)[mapKey]
		if ok {
			break
		}
		lastIndex := strings.LastIndex(mapKey, ".")
		if lastIndex > -1 {
			mapKey = mapKey[:lastIndex]
			continue
		}
		break
	}
	if mapKey == key {
		return value, ok
	}
	// json key 获取值
	jsonKey = key[len(mapKey)+1:]
	jsonStr, ok := value.(string)
	if !ok {
		return nil, false
	}
	jsonValue, ok := GetValueFromJson(jsonStr, jsonKey)
	return jsonValue, ok
}

func GetValueFromJson(jsonStr string, jsonKey string) (interface{}, bool) {
	if jsonStr == "" {
		return nil, false
	}
	if !gjson.Valid(jsonStr) {
		err := errors.Errorf(`json str inValid %s`, jsonStr)
		panic(err)
	}
	key := jsonKey
	value := gjson.Result{}
	for {
		value = gjson.Get(jsonStr, key)
		if value.Exists() {
			break
		}
		lastIndex := strings.LastIndex(key, ".")
		if lastIndex > -1 {
			key = key[:lastIndex]
			continue
		}
		break
	}
	if jsonKey == key {
		return value.Value(), value.Exists()
	}

	return GetValueFromJson(value.Str, jsonKey[len(key)+1:])
}

func convertType(dst interface{}, src interface{}) bool {
	if src == nil || dst == nil {
		return false
	}
	rv := reflect.Indirect(reflect.ValueOf(dst))
	if !rv.CanSet() {
		err := errors.Errorf("dst :%#v reflect.CanSet() must return  true", dst)
		panic(err)
	}
	rvT := rv.Type()

	rTmp := reflect.ValueOf(src)
	if rTmp.CanConvert(rvT) {
		realValue := rTmp.Convert(rvT)
		rv.Set(realValue)
		return true
	}
	srcStr := strval(src)
	switch rvT.Kind() {
	case reflect.Int:
		srcInt, err := strconv.Atoi(srcStr)
		if err != nil {
			err = errors.WithMessagef(err, "src:%s can`t convert to int", srcStr)
			panic(err)
		}
		rv.Set(reflect.ValueOf(srcInt))
		return true
	case reflect.Int64:
		srcInt, err := strconv.ParseInt(srcStr, 10, 64)
		if err != nil {
			err = errors.WithMessagef(err, "src:%s can`t convert to int64", srcStr)
			panic(err)
		}
		rv.SetInt(int64(srcInt))
		return true
	case reflect.Float64:
		srcFloat, err := strconv.ParseFloat(srcStr, 64)
		if err != nil {
			err = errors.WithMessagef(err, "src:%s can`t convert to float64", srcStr)
			panic(err)
		}
		rv.SetFloat(srcFloat)
		return true
	case reflect.Bool:
		srcBool, err := strconv.ParseBool(srcStr)
		if err != nil {
			err = errors.WithMessagef(err, "src:%s can`t convert to bool", srcStr)
			panic(err)
		}
		rv.SetBool(srcBool)
		return true
	case reflect.String:
		rv.SetString(srcStr)
		return true

	}

	err := errors.Errorf("can not convert %v(%s) to %#v", src, rTmp.Type().String(), rvT.String())
	panic(err)
}
