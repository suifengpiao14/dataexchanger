package source

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
)

func TestSplitN(t *testing.T) {
	input := `
	#!/usr/bin/php
	`
	input = util.StandardizeSpaces(input)
	input = strings.ReplaceAll(input, WINDOW_EOF, EOF)
	script := strings.SplitN(input, EOF, 2)
	if len(script) < 1 {
		err := errors.Errorf("command line required")
		panic(err)
	}
	body := strings.NewReader(script[1])
	b, _ := io.ReadAll(body)
	fmt.Printf(string(b))
}

func TestBinProvider(t *testing.T) {
	binExecProvider := &BinExecProvider{}
	input := `
	ping -n 3 
	baidu.com
	`
	out, err := binProvider(binExecProvider, input)
	if err != nil {
		panic(err)
	}
	fmt.Printf(out)
}
