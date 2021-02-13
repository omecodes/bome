package bome

import (
	"database/sql"
	"github.com/google/uuid"
	"strings"
)

// TX is a transaction token
type TX struct {
	id string
	db *DB
	*sql.Tx
}

// NewTransaction initialize a TX object and generate its id
func NewTransaction() *TX {
	return &TX{
		id: uuid.New().String(),
	}
}

//Exec executes the statement saved as name
func (tx *TX) Exec(query string, args ...interface{}) Result {
	for name, value := range tx.db.vars {
		query = strings.Replace(query, name, value, -1)
	}

	var r sql.Result
	result := Result{}
	r, result.Error = tx.Tx.Exec(query, args...)
	if result.Error == nil && tx.db.dialect != SQLite3 {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

//Query executes the query statement saved as name
func (tx *TX) Query(query string, scannerName string, args ...interface{}) (Cursor, error) {
	for name, value := range tx.db.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := tx.Tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	scanner, err := tx.db.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

// QueryFirst get the first result of the query statement saved as name
func (tx *TX) QueryFirst(query string, scannerName string, args ...interface{}) (interface{}, error) {
	for name, value := range tx.db.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := tx.Tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	scanner, err := tx.db.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

// Commit commits the transaction
func (tx *TX) Commit() error {
	return tx.Tx.Commit()
}

// Rollback reverts all changes operated during the transaction
func (tx *TX) Rollback() error {
	return tx.Tx.Rollback()
}
