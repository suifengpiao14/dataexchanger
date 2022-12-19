package datacenter

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/xeipuuv/gojsonschema"
)

var (
	rxPhone    = regexp.MustCompile(`/^1[3456789]\d{9}$/`)
	rxIdCard   = regexp.MustCompile(`/^[1-9]\d{5}(18|19|([23]\d))\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\d{3}[0-9Xx]$/`)
	rxIdCard1  = regexp.MustCompile(`/^[1-9]\d{5}\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\d{3}$/`)
	rxPostCode = regexp.MustCompile(`/^[1-9]{1}(\d+){5}$/`)
)

func RegisterFormatChecker() {
	gojsonschema.FormatCheckers.Add("number", NumberFormatChecker{}) // 数字格式验证
	gojsonschema.FormatCheckers.Add("phone", NumberFormatChecker{})  // 数字格式验证
}

type NumberFormatChecker struct{}

// IsFormat checks if input is a correctly formatted number string
func (f NumberFormatChecker) IsFormat(input interface{}) bool {
	asString, ok := input.(string)
	if !ok {
		return false
	}
	_, err := strconv.ParseFloat(asString, 64)
	return err == nil
}

type PhoneFormatChecker struct{}

// IsFormat checks if input is a correctly formatted phone string
func (f PhoneFormatChecker) IsFormat(input interface{}) bool {
	asString, ok := input.(string)
	if !ok {
		return false
	}
	out := rxPhone.MatchString(asString)
	return out
}

type IDCardFormatChecker struct{}

// IsFormat checks if input is a correctly formatted IDCard string
func (f IDCardFormatChecker) IsFormat(input interface{}) bool {
	asString, ok := input.(string)
	if !ok {
		return false
	}
	out := rxIdCard.MatchString(asString) || rxIdCard1.MatchString(asString)
	return out
}

type PostCodeFormatChecker struct{}

// IsFormat checks if input is a correctly formatted postcode string
func (f PostCodeFormatChecker) IsFormat(input interface{}) bool {
	asString, ok := input.(string)
	if !ok {
		return false
	}
	out := rxPostCode.MatchString(asString)
	return out
}

func Validate(input string, jsonLoader gojsonschema.JSONLoader) (err error) {
	if input == "" {
		jsonschema, err := jsonLoader.LoadJSON()
		if err != nil {
			return err
		}
		jsonMap, ok := jsonschema.(map[string]interface{})
		if !ok {
			err = errors.Errorf("can not convert jsonLoader.LoadJSON() to map[string]interface{}")
			return err
		}
		typ, ok := jsonMap["type"]
		if !ok {
			err = errors.Errorf("jsonschema missing property type :%v", jsonschema)
			return err
		}
		typStr, ok := typ.(string)
		if !ok {
			err = errors.Errorf("can not convert  jsonschema type to string :%v", typ)
			return err

		}
		switch strings.ToLower(typStr) {
		case "object":
			input = "{}"
		case "array":
			input = "[]"
		default:
			err = errors.Errorf("invalid jsonschema type:%v", typStr)
			return err
		}

	}
	documentLoader := gojsonschema.NewStringLoader(input)
	result, err := gojsonschema.Validate(jsonLoader, documentLoader)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}

	msgArr := make([]string, 0)
	for _, resultError := range result.Errors() {
		msgArr = append(msgArr, resultError.String())
	}
	err = errors.Errorf("input args validate errors: %s", strings.Join(msgArr, ","))
	return err
}
