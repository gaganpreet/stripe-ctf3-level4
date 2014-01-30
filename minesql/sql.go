package minesql

import (
	"os/exec"
	"strings"
	"stripe-ctf.com/sqlcluster/log"
	"sync"
	"syscall"
    "fmt"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

type MineSQL struct {
	Path           string
	sequenceNumber int
	mutex          sync.Mutex
    sqlH           *sql.DB
    Cache          map[string]string
}

type Output struct {
	Stdout         []byte
	Stderr         []byte
	SequenceNumber int
}

func NewSQL(path string) *MineSQL {
    sqlH, err := sql.Open("sqlite3", ":memory:") //path + "2")
    log.Printf("Creating memory database: <<<%v>>> <<<%v>>> <<<%v>>>", err, sqlH, err == nil)
	sql := &MineSQL{
		Path: path,
        sqlH: sqlH,
        Cache: make(map[string]string),
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

func (db *MineSQL) Execute(command string) (string, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	defer func() { db.sequenceNumber += 1 }()
    // log.Printf("[%d] Executing %s", db.sequenceNumber, command)

    var (
        a string
        b int
        c int
        d string
    )

    formatted := ""
    queries := strings.Split(command, ";")
    for _, query := range queries {
        if strings.Contains(query, "UPDATE") || strings.Contains(query, "INSERT") || strings.Contains(query, "CREATE") {
            _, _ = db.sqlH.Exec(query)
            continue
        }

        if strings.Contains(query, "SELECT") {
            rows, err := db.sqlH.Query(query)
            if err != nil {
                panic(err.Error())
            }

            for rows.Next() {
                _ = rows.Scan(&a, &b, &c, &d)
                formatted += fmt.Sprintf("%s|%d|%d|%s\n", a, b, c, d)
            }
        }
    }

    formatted = fmt.Sprintf("SequenceNumber: %d\n%s",
    db.sequenceNumber, formatted)
    db.Cache[command] = formatted

	return formatted, nil
}
