package source

import (
	"io"
	"io/ioutil"
	"os/exec"
	"strings"
	"sync"

	shellwords "github.com/mattn/go-shellwords"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/datacenter/util"
)

type BinExecProvider struct {
}

func (p *BinExecProvider) Exec(s string) (string, error) {
	return binProvider(p, s)
}

func (p *BinExecProvider) GetSource() (source interface{}) {
	err := errors.Errorf("BinExec no dependence source")
	panic(err)
}

func binProvider(p *BinExecProvider, input string) (string, error) {
	// Start subprocess
	input = util.StandardizeSpaces(input)
	input = strings.ReplaceAll(input, WINDOW_EOF, EOF)
	script := strings.SplitN(input, EOF, 2)
	if len(script) < 1 {
		err := errors.Errorf("command line required")
		return "", err
	}
	command := script[0]
	var bodyStr string
	if len(script) == 2 {
		bodyStr = script[1]
	}
	body := strings.NewReader(bodyStr)

	args, err := shellwords.Parse(command)
	if err != nil {
		err = errors.WithMessagef(err, "error parsing SCRIPT environment variable: %v", err)
		return "", err
	}
	var cmd *exec.Cmd
	cmd = exec.Command(args[0], args[1:]...)

	// Get handles to subprocess stdin, stdout and stderr
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		err = errors.WithMessagef(err, "error accessing subprocess stdin: %v", err)
		return "", nil
	}
	defer stdinPipe.Close()
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		err = errors.WithMessagef(err, "error accessing subprocess stderr: %v", err)
		return "", err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		err = errors.WithMessagef(err, "error accessing subprocess stdout: %v", err)
		return "", err
	}

	// Start the subprocess
	err = cmd.Start()
	if err != nil {
		err = errors.WithMessagef(err, "error starting subprocess: %v", err)
		return "", err
	}

	// We use a WaitGroup to wait for all goroutines to finish
	wg := sync.WaitGroup{}

	// Write request body to subprocess stdin
	wg.Add(1)
	go func() {
		defer func() {
			stdinPipe.Close()
			wg.Done()
		}()
		_, err = io.Copy(stdinPipe, body)
		if err != nil {
			err = errors.WithMessagef(err, "error writing request body to subprocess stdin: %v", err)
			return
		}
	}()

	// Read all stderr and write to parent stderr if not empty
	wg.Add(1)
	go func() {
		defer wg.Done()
		stderr, err := ioutil.ReadAll(stderrPipe)
		if err != nil {
			err = errors.WithMessagef(err, "error reading subprocess stderr: %v", err)
			return
		}
		if len(stderr) > 0 {
			err = errors.Errorf("stderr out put error:%s", string(stderr))
		}
	}()

	// Read all stdout, but don't write to the response as we need the exit
	// status of the subcommand to know our HTTP response code
	wg.Add(1)
	var stdout []byte
	go func() {
		defer wg.Done()
		so, err := ioutil.ReadAll(stdoutPipe)
		stdout = so
		if err != nil {
			err = errors.WithMessagef(err, "error reading subprocess stdout: %v", err)
			return
		}
	}()

	// We must consume stdout and stderr before `cmd.Wait()` as per
	// doc and example at https://golang.org/pkg/os/exec/#Cmd.StdoutPipe
	wg.Wait()

	// Wait for the subprocess to complete
	cmdErr := cmd.Wait()
	if cmdErr != nil {
		// We don't return here because we also want to try to write stdout if
		// there was some output
		err = errors.WithMessagef(err, "error running subprocess: %v", err)
	}

	return string(stdout), err
}
