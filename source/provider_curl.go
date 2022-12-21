package source

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
)

const (
	EOF                  = "\n"
	WINDOW_EOF           = "\r\n"
	HTTP_HEAD_BODY_DELIM = EOF + EOF
)

var CURL_TIMEOUT = 30 * time.Millisecond

type RequestData struct {
	URL     string         `json:"url"`
	Method  string         `json:"method"`
	Header  http.Header    `json:"header"`
	Cookies []*http.Cookie `json:"cookies"`
	Body    string         `json:"body"`
}
type ResponseData struct {
	HttpStatus  string         `json:"httpStatus"`
	Header      http.Header    `json:"header"`
	Cookies     []*http.Cookie `json:"cookies"`
	Body        string         `json:"body"`
	RequestData *RequestData   `json:"requestData"`
}

type CURLExecProviderConfig struct {
	Proxy               string `json:"proxy"`
	LogLevel            string `json:"logLevel"`
	Timeout             int    `json:"timeout"`
	KeepAlive           int    `json:"keepAlive"`
	MaxIdleConns        int    `json:"maxIdleConns"`
	MaxIdleConnsPerHost int    `json:"maxIdleConnsPerHost"`
	IdleConnTimeout     int    `json:"idleConnTimeout"`
}

type CURLExecProvider struct {
	Config     CURLExecProviderConfig
	client     *http.Client
	clinetOnce sync.Once
}

func (p *CURLExecProvider) Exec(s string) (string, error) {
	return CURlProvider(p, s)
}

func (p *CURLExecProvider) GetSource() (source interface{}) {
	return p.client
}

func (p *CURLExecProvider) Rollback(tx interface{}) (err error) {
	return nil
}

func (p *CURLExecProvider) Commit(tx interface{}) (err error) {
	return nil
}

// GetDb is a signal DB
func (p *CURLExecProvider) GetClient() *http.Client {
	if p.client == nil {
		p.clinetOnce.Do(func() {
			p.client = InitHTTPClient(p)

		})
	}
	return p.client
}

func InitHTTPClient(p *CURLExecProvider) *http.Client {

	maxIdleConns := 200
	maxIdleConnsPerHost := 20
	idleConnTimeout := 90
	if p.Config.MaxIdleConns > 0 {
		maxIdleConns = p.Config.MaxIdleConns
	}
	if p.Config.MaxIdleConnsPerHost > 0 {
		maxIdleConnsPerHost = p.Config.MaxIdleConnsPerHost
	}
	if p.Config.IdleConnTimeout > 0 {
		idleConnTimeout = p.Config.IdleConnTimeout
	}
	timeout := 10
	if p.Config.Timeout > 0 {
		timeout = 10
	}
	keepAlive := 300
	if p.Config.KeepAlive > 0 {
		keepAlive = p.Config.KeepAlive
	}
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(timeout) * time.Second,   // 连接超时时间
			KeepAlive: time.Duration(keepAlive) * time.Second, // 连接保持超时时间
		}).DialContext,
		MaxIdleConns:        maxIdleConns,                                 // 最大连接数,默认0无穷大
		MaxIdleConnsPerHost: maxIdleConnsPerHost,                          // 对每个host的最大连接数量(MaxIdleConnsPerHost<=MaxIdleConns)
		IdleConnTimeout:     time.Duration(idleConnTimeout) * time.Second, // 多长时间未使用自动关闭连
	}
	if p.Config.Proxy != "" {
		proxy, err := url.Parse(p.Config.Proxy)
		if err != nil {
			panic(err)
		}
		transport.Proxy = http.ProxyURL(proxy)
	}
	httpClient := &http.Client{
		Transport: transport,
	}
	return httpClient
}

func CURlProvider(p *CURLExecProvider, httpRaw string) (string, error) {
	reqReader, err := ReadRequest(httpRaw)
	if err != nil {
		return "", err
	}
	reqData, err := Request2RequestData(reqReader)
	if err != nil {
		return "", err
	}
	timeout := 30
	if p.Config.Timeout > 0 {
		timeout = p.Config.Timeout
	}
	timeoutStr := reqReader.Header.Get("x-http-timeout")
	if timeoutStr != "" {
		timeoutInt, _ := strconv.Atoi(timeoutStr)
		if timeoutInt > 0 {
			timeout = timeoutInt // 优先使用定制化的超时时间
		}
	}
	timeoutDuration := time.Duration(timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, reqData.Method, reqData.URL, bytes.NewReader([]byte(reqData.Body)))
	if err != nil {
		return "", err
	}

	for k, vArr := range reqData.Header {
		for _, v := range vArr {
			req.Header.Add(k, v)
		}
	}

	rsp, err := p.GetClient().Do(req)
	if err != nil {
		return "", err
	}

	defer rsp.Body.Close()
	b, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", err
	}
	if err != nil {
		msg := fmt.Sprintf("response httpstatus:%d, body: %s", rsp.StatusCode, string(b))
		err = errors.WithMessage(err, msg)
		return "", err
	}
	if rsp.StatusCode != http.StatusOK {
		err := errors.Errorf("response httpstatus:%d, body: %s", rsp.StatusCode, string(b))
		return "", err
	}

	rspData := ResponseData{
		HttpStatus:  strconv.Itoa(rsp.StatusCode),
		Header:      rsp.Header,
		Cookies:     rsp.Cookies(),
		RequestData: reqData,
	}
	rspData.Body = string(b)
	jsonByte, err := json.Marshal(rspData)
	if err != nil {
		return "", err
	}
	out := string(jsonByte)

	return out, nil
}

func ReadRequest(httpRaw string) (req *http.Request, err error) {
	httpRaw = util.TrimSpaces(httpRaw) // （删除前后空格，对于没有body 内容的请求，后面再加上必要的换行）
	if httpRaw == "" {
		err = errors.Errorf("http raw not allow empty")
		return nil, err
	}
	httpRaw = strings.ReplaceAll(httpRaw, "\r\n", "\n") // 统一换行符
	// 插入body长度头部信息
	bodyIndex := strings.Index(httpRaw, HTTP_HEAD_BODY_DELIM)
	formatHttpRaw := httpRaw
	if bodyIndex > 0 {
		headerRaw := strings.TrimSpace(httpRaw[:bodyIndex])
		bodyRaw := httpRaw[bodyIndex+len(HTTP_HEAD_BODY_DELIM):]
		bodyLen := len(bodyRaw)
		formatHttpRaw = fmt.Sprintf("%s%sContent-Length: %d%s%s", headerRaw, EOF, bodyLen, HTTP_HEAD_BODY_DELIM, bodyRaw)
	} else {
		// 如果没有请求体，则原始字符后面必须保留一个换行符
		formatHttpRaw = fmt.Sprintf("%s%s", formatHttpRaw, HTTP_HEAD_BODY_DELIM)
	}

	buf := bufio.NewReader(strings.NewReader(formatHttpRaw))
	req, err = http.ReadRequest(buf)
	if err != nil {
		return
	}
	if req.URL.Scheme == "" {
		queryPre := ""
		if req.URL.RawQuery != "" {
			queryPre = "?"
		}
		req.RequestURI = fmt.Sprintf("http://%s%s%s%s", req.Host, req.URL.Path, queryPre, req.URL.RawQuery)
	}

	return
}

func Request2RequestData(req *http.Request) (requestData *RequestData, err error) {
	requestData = &RequestData{}
	bodyByte, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}
	req.Header.Del("Content-Length")
	requestData = &RequestData{
		URL:     req.URL.String(),
		Method:  req.Method,
		Header:  req.Header,
		Cookies: req.Cookies(),
		Body:    string(bodyByte),
	}

	return
}
