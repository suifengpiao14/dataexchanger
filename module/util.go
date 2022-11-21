package module

import "strings"

func StandardizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

