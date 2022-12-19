package datacenter

import (
	"text/template"
)

// GetTemplateNames 获取模板名称
func GetTemplateNames(t *template.Template) []string {
	out := make([]string, 0)
	for _, tpl := range t.Templates() {
		name := tpl.Name()
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}
