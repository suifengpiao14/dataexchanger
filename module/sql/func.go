package sql

import "text/template"

var TemplatefuncMap = template.FuncMap{
	"newPreComma": NewPreComma,
}

type preComma struct {
	comma string
}

func NewPreComma() *preComma {
	return &preComma{}
}

func (c *preComma) PreComma() string {
	out := c.comma
	c.comma = ","
	return out
}
