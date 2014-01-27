package command

import (
        "github.com/goraft/raft"
        "stripe-ctf.com/sqlcluster/sql"
)

// This command writes a value to a key.
type WriteCommand struct {
        SQL []byte `json:"value"`
}

// Creates a new write command.
func NewWriteCommand(sql []byte) *WriteCommand {
        return &WriteCommand{
                SQL:   sql,
        }
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
        return "write"
}

// Writes a value to a key.
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
        db := server.Context().(*sql.SQL)
        output, err := db.Execute(string(c.SQL))
        return output, err
}
