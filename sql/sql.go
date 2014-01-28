package sql

import (
	"bytes"
	"os/exec"
	"strings"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/util"
	"sync"
	"syscall"
    "fmt"
    "io/ioutil"
)

type SQL struct {
	Path           string
	sequenceNumber int
	mutex          sync.Mutex
}

type Output struct {
	Stdout         []byte
	Stderr         []byte
	SequenceNumber int
}

func NewSQL(path string) *SQL {
	sql := &SQL{
		Path: path,
	}
	return sql
}

func getExitstatus(err error) int {
	exiterr, ok := err.(*exec.ExitError)
	if !ok {
		return -1
	}

	status, ok := exiterr.Sys().(syscall.WaitStatus)
	if !ok {
		return -1
	}

	return status.ExitStatus()
}

func (sql *SQL) Execute(command string) (string, error) {
	// TODO: make sure I can catch non-lock issuez
	sql.mutex.Lock()
	defer sql.mutex.Unlock()

	defer func() { sql.sequenceNumber += 1 }()
    log.Printf("[%d] Executing %s", sql.sequenceNumber, command)

    filename := util.Sha1(command)
    filename = sql.Path + filename

    log.Printf("filename: ", filename)
    if util.Exists(filename) == true {
        contents, _ := ioutil.ReadFile(filename)
        if len(contents) > 0 && strings.Contains(string(contents), "no such table") == false {
            log.Printf("returning for %s -> %s", command, string(contents))
            return string(contents), nil
        }
    }


	subprocess := exec.Command("sqlite3", sql.Path)
	subprocess.Stdin = strings.NewReader(command + ";")

    log.Printf("1")
	var stdout, stderr bytes.Buffer
	subprocess.Stdout = &stdout
	subprocess.Stderr = &stderr
    log.Printf("2")

	if err := subprocess.Start(); err != nil {
		log.Panic(err)
	}

	var o, e []byte
    log.Printf("3")

	if err := subprocess.Wait(); err != nil {
		exitstatus := getExitstatus(err)
        log.Printf("Exit status: ", exitstatus)
		switch true {
		case exitstatus < 0:
			log.Panic(err)
		case exitstatus == 1:
			fallthrough
		case exitstatus == 2:
			o = stderr.Bytes()
			e = nil
		}
	} else {
		o = stdout.Bytes()
		e = stderr.Bytes()
	}
    log.Printf("<%s>", o)
    log.Printf("<%s>", e)

	output := &Output{
		Stdout:         o,
		Stderr:         e,
		SequenceNumber: sql.sequenceNumber,
	}

    formatted := fmt.Sprintf("SequenceNumber: %d\n%s%s",
    output.SequenceNumber, output.Stdout, output.Stderr)
    if util.Exists(filename) == false {
        ioutil.WriteFile(filename, []byte(formatted), 777)
    }
    log.Printf("4")

	return formatted, nil
}
