package command

import (
	"github.com/goraft/raft"
	"stripe-ctf.com/sqlcluster/minesql"
	"stripe-ctf.com/sqlcluster/util"
)

// This command writes a value to a key.
type Command struct {
    Sql string `json:"value"`
}

// Creates a new write command.
func NewWriteCommand(sql string) *Command {
    // fmt.Printf("Creating new command ", sql)
	return &Command{
		Sql: util.Compress(sql, true),
	}
}

// The name of the command in the log.
func (c *Command) CommandName() string {
	return "write"
}

// Writes a value to a key.
func (c *Command) Apply(server raft.Server) (interface{}, error) {
    // fmt.Printf("Command Apply %#v", c)
	db := server.Context().(*minesql.MineSQL)
    output, err := db.Execute(util.Compress(c.Sql, false))
	return output, err
}
