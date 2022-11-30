package gsjson

import (
	"fmt"
	"testing"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/stretchr/testify/require"
)

func TestCombine(t *testing.T) {
	jsonstr := `
	{"_errCode":"0","_errStr":"SUCCESS","_data":{"items":[{"id":"1","qid":"12057","qname":"全新机(包装盒无破损,配件齐全且原装,可无原 机膜和卡针)","classId":"1","className":"手机","step":"3","stepName":"维修情况","sort":"1","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-11-28 14:33:59"},{"id":"2","qid":"12097","qname":"机身弯曲情况","classId":"3","className":"平板","step":"2","stepName":"成色情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"3","qid":"12066","qname":"屏幕外观","classId":"3","className":"平板","step":"2","stepName":"成色情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"4","qid":"12077","qname":"屏幕显示","classId":"3","className":"平板","step":"2","stepName":"成色情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"5","qid":"12088","qname":"电池健康效率","classId":"3","className":"平板","step":"3","stepName":"维修情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"6","qid":"12100","qname":"维修情况","classId":"3","className":"平板","step":"3","stepName":"维修情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"7","qid":"12106","qname":"零件维修情况","classId":"3","className":"平板","step":"3","stepName":"维修情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"8","qid":"12115","qname":"受潮状况","classId":"3","className":"平板","step":"3","stepName":"维修情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"9","qid":"12119","qname":"开机状态","classId":"3","className":"平板","step":"4","stepName":"功能情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"},{"id":"10","qid":"9368","qname":"是否全新","classId":"3","className":"平板","step":"4","stepName":"功能情况","sort":"0","isMultiple":"0","createTime":"2022-08-09 11:50:22","updateTime":"2022-08-09 11:50:22"}],"pageInfo":{"pageIndex":"0","pageSize":"10","total":"164"}}}
	`
	jsonObj := &tengo.String{Value: jsonstr}
	pathMap := `
	["classId":_data.items.#.classId,"className":_data.items.#.className]|@combine
	`
	pathObj := &tengo.String{Value: pathMap}
	s := tengo.NewScript([]byte(`
	out:=GSjson.Get(jsonstr,path)
	`))
	s.EnableFileImport(true)
	err := s.Add("GSjson", GSjon)
	if err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr", jsonObj); err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("path", pathObj); err != nil {
		require.NoError(t, err)
		return
	}

	c, err := s.Compile()
	if err != nil {
		require.NoError(t, err)
		return
	}

	if err := c.Run(); err != nil {
		require.NoError(t, err)
		return
	}

	v := c.Get("out")
	fmt.Println(v)

}

func TestLeftJoin(t *testing.T) {
	jsonstr := `
	[{"questionId":"12057","questionName":"全新机(包装盒无破损,配件齐全且原装,可无原 机膜和卡针)","classId":"1"},{"questionId":"12097","questionName":"机身弯曲情况","classId":"3"},{"questionId":"12066","questionName":"屏幕外观","classId":"3"},{"questionId":"12077","questionName":"屏幕显示","classId":"3"},{"questionId":"12088","questionName":"电池健康效率","classId":"3"},{"questionId":"12100","questionName":"维修情况","classId":"3"},{"questionId":"12106","questionName":"零件维修情况","classId":"3"},{"questionId":"12115","questionName":"受潮状况","classId":"3"},{"questionId":"12119","questionName":"开机状态","classId":"3"},{"questionId":"9368","questionName":"是否全新","classId":"3"}]
	`
	jsonstr2 := `
	[{"classId":"1","className":"手机"},{"classId":"3","className":"平板"}]
	`
	jsonObj := &tengo.String{Value: jsonstr}
	jsonObj2 := &tengo.String{Value: jsonstr2}

	s := tengo.NewScript([]byte(`
	fmt:=import("fmt")
	jstr:=fmt.sprintf("[%s,%s]",jsonstr,jsonstr2)
	path:="@leftJoin:[@this.0.#.classId,@this.1.#.classId]"
	out:=GSjson.Get(jstr,path)
	`))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err := s.Add("GSjson", GSjon)
	if err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr", jsonObj); err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr2", jsonObj2); err != nil {
		require.NoError(t, err)
		return
	}

	c, err := s.Compile()
	if err != nil {
		require.NoError(t, err)
		return
	}

	if err := c.Run(); err != nil {
		require.NoError(t, err)
		return
	}

	v := c.Get("out")
	fmt.Println(v)

}

func TestGetSet(t *testing.T) {
	jsonstr := `
	[{"questionId":"12057","questionName":"全新机(包装盒无破损,配件齐全且原装,可无原 机膜和卡针)","classId":"1","className":"手机"},{"questionId":"12097","questionName":"机身弯曲情况","classId":"3","className":"平板"}]
	`
	jsonObj := &tengo.String{Value: jsonstr}
	pathMapObj := &tengo.String{
		Value: `[{"src":"@this.#.questionId","dst":"items.-1.id"},{"src":"@this.#.questionName","dst":"items.-1.name"},{"src":"@this.#.classId","dst":"items.-1.classId"},{"src":"@this.#.className","dst":"items.-1.className"}]`,
	}
	s := tengo.NewScript([]byte(`
	fmt:=import("fmt")
	out:=GSjson.GetSet(jsonstr,pathMap)
	`))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err := s.Add("GSjson", GSjon)
	if err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr", jsonObj); err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("pathMap", pathMapObj); err != nil {
		require.NoError(t, err)
		return
	}

	c, err := s.Compile()
	if err != nil {
		require.NoError(t, err)
		return
	}

	if err := c.Run(); err != nil {
		require.NoError(t, err)
		return
	}

	v := c.Get("out")
	fmt.Println(v)

}

func TestGetSetArr(t *testing.T) {
	jsonstr := `
	[{"questionId":"12057","questionName":"全新机(包装盒无破损,配件齐全且原装,可无原 机膜和卡针)","classId":"1","className":"手机"}],
	[{"questionId":"12097","questionName":"机身弯曲情况","classId":"3","className":"平板"}]
	`
	jsonObj := &tengo.String{Value: jsonstr}
	pathMapObj := &tengo.String{
		Value: `[{"src":"@this.#.questionId","dst":"items.-1.id"},{"src":"@this.#.questionName","dst":"items.-1.name"},{"src":"@this.#.classId","dst":"items.-1.classId"},{"src":"@this.#.className","dst":"items.-1.className"}]`,
	}
	s := tengo.NewScript([]byte(`
	fmt:=import("fmt")
	out:=GSjson.GetSet(jsonstr,pathMap)
	`))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err := s.Add("GSjson", GSjon)
	if err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr", jsonObj); err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("pathMap", pathMapObj); err != nil {
		require.NoError(t, err)
		return
	}

	c, err := s.Compile()
	if err != nil {
		require.NoError(t, err)
		return
	}

	if err := c.Run(); err != nil {
		require.NoError(t, err)
		return
	}

	v := c.Get("out")
	fmt.Println(v)

}

func TestIndex(t *testing.T) {
	jsonstr := `
	[{"questionId":"12057","questionName":"全新机(包装盒无破损,配件齐全且原装,可无原 机膜和卡针)","classId":"1"},{"questionId":"12097","questionName":"机身弯曲情况","classId":"3"},{"questionId":"12066","questionName":"屏幕外观","classId":"3"},{"questionId":"12077","questionName":"屏幕显示","classId":"3"},{"questionId":"12088","questionName":"电池健康效率","classId":"3"},{"questionId":"12100","questionName":"维修情况","classId":"3"},{"questionId":"12106","questionName":"零件维修情况","classId":"3"},{"questionId":"12115","questionName":"受潮状况","classId":"3"},{"questionId":"12119","questionName":"开机状态","classId":"3"},{"questionId":"9368","questionName":"是否全新","classId":"3"}]
	`
	jsonObj := &tengo.String{Value: jsonstr}
	pathObj := &tengo.String{
		Value: `@this|@index:#.classId`,
	}
	s := tengo.NewScript([]byte(`
	fmt:=import("fmt")
	out:=GSjson.Get(jsonstr,path)
	`))
	s.EnableFileImport(true)
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	err := s.Add("GSjson", GSjon)
	if err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("jsonstr", jsonObj); err != nil {
		require.NoError(t, err)
		return
	}
	if err = s.Add("path", pathObj); err != nil {
		require.NoError(t, err)
		return
	}

	c, err := s.Compile()
	if err != nil {
		require.NoError(t, err)
		return
	}

	if err := c.Run(); err != nil {
		require.NoError(t, err)
		return
	}

	v := c.Get("out")
	fmt.Println(v)
}
