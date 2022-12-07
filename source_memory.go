package datacenter

import "github.com/pkg/errors"

type MemorySource struct {
	InOutMap map[string]string
}

func (m *MemorySource) Exec(s string) (out string, err error) {
	out, ok := m.InOutMap[s]
	if !ok {
		err = errors.Errorf("not found by in:%s", s)
		return "", err
	}
	return out, nil
}

func (m *MemorySource) GetSource() (source interface{}) {
	return m.InOutMap
}
