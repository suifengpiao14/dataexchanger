package util

import "strings"

func StandardizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TrimSpaces  去除开头结尾的非有效字符
func TrimSpaces(s string) string {
	return strings.Trim(s, "\r\n\t\v\f ")
}
