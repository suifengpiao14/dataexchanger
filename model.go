package datacenter

import datatemplate "github.com/suifengpiao14/datacenter/module/template"

type Model struct {
	TemplateID    string
	CatchSourceID string
	EventSourceID string
	DBSourceID    string
	HTTPSourceID  string
	sourcePool    *SourcePool
	template      *datatemplate.Template
}

// Exec 执行领域内调度逻辑,如缓存数据、更新数据、发布事件等(查询模型时设置缓存, 更新模型时,更新缓存、发布数据)
func (m *Model) Exec() {

}
