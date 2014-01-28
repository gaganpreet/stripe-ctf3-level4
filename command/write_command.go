package command

import (
    "fmt"
	"github.com/goraft/raft"
	"stripe-ctf.com/sqlcluster/sql"
)

// This command writes a value to a key.
type Command struct {
    Sql string `json:"value"`
}

// Creates a new write command.
func NewWriteCommand(sql string) *Command {
    fmt.Printf("Creating new command ", sql)
	return &Command{
		Sql: sql,
	}
}

// The name of the command in the log.
func (c *Command) CommandName() string {
	return "write"
}

// Writes a value to a key.
func (c *Command) Apply(server raft.Server) (interface{}, error) {
    fmt.Printf("Command Apply %#v", c)
	db := server.Context().(*sql.SQL)
    output, err := db.Execute(c.Sql)
	return output, err
}
