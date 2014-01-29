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
    //"code.google.com/p/go-sqlite/"
//    "code.google.com/p/go-sqlite/go1/sqlite3"
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
	// TODO: make sure I can catch non-lock issuez
	db.mutex.Lock()
	defer db.mutex.Unlock()

	defer func() { db.sequenceNumber += 1 }()
    // log.Printf("[%d] Executing %s", db.sequenceNumber, command)

    /*
    filename := util.Sha1(command)
    filename = db.Path + filename

    cached_val, ok := db.Cache[command]
    if ok {
        return cached_val, nil
    }
    */

    /*
    log.Printf("filename: ", filename)
    if util.Exists(filename) == true {
        contents, _ := ioutil.ReadFile(filename)
        if len(contents) > 0 && strings.Contains(string(contents), "no such table") == false {
            log.Printf("returning for %s -> %s", command, string(contents))
            return string(contents), nil
        }
    }
    */
    var (
 //       container []string
 //       pointers  []interface{}
        a string
        b int
        c int
        d string
    )

    formatted := ""
    // fmt.Println("library", command)
    queries := strings.Split(command, ";")
    for _, query := range queries {
    //     log.Printf("query mem ", query)
        if strings.Contains(query, "UPDATE") || strings.Contains(query, "INSERT") || strings.Contains(query, "CREATE") {
            _, _ = db.sqlH.Exec(query)
            continue
        }



        if strings.Contains(query, "SELECT") {
            rows, err := db.sqlH.Query(query)
            if err != nil {
                panic(err.Error())
            }

//            length := 4

            for rows.Next() {
                _ = rows.Scan(&a, &b, &c, &d)
                /*
                pointers = make([]interface{}, length)
                container = make([]string, length)

                for i := range pointers {
                    pointers[i] = &container[i]
                }

                err = rows.Scan(pointers...)
                if err != nil {
                    panic(err.Error())
                }
                */
                formatted += fmt.Sprintf("%s|%d|%d|%s", a, b, c, d) + "\n"
            }

        //    cols, _ := rows.Columns()
        //    rawResult := make([][]byte, len(cols))
        //    result := make([]string, len(cols))
        //    dest := make([]interface{}, len(cols))

        //    for i, _ := range rawResult {
        //        dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
        //    }

        //    for rows.Next() {
        //        _ = rows.Scan(dest...)
        //        for i, raw := range dest {
        //            if raw == nil {
        //                result[i] += "\n"
        //            } else {
        //                result[i] = string(raw.(*[]byte))
        //            }
        //        }
        //    }
        }
    }

    /*
	subprocess := exec.Command("sqlite3", db.Path)
	subprocess.Stdin = strings.NewReader(command + ";")

	var stdout, stderr bytes.Buffer
	subprocess.Stdout = &stdout
	subprocess.Stderr = &stderr

	if err := subprocess.Start(); err != nil {
		log.Panic(err)
	}

	var o, e []byte

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
	output := &Output{
		Stdout:         o,
		Stderr:         e,
		SequenceNumber: db.sequenceNumber,
	}

    */

    formatted = fmt.Sprintf("SequenceNumber: %d\n%s",
    db.sequenceNumber, formatted)
    db.Cache[command] = formatted

	return formatted, nil
}
