package bome

import (
	"database/sql"
	"strings"
)

type transaction interface {
	Commit() error
	Rollback() error
	TX() *TX
}

// TX is a transaction token
type TX struct {
	dbome *Bome
	*sql.Tx
}

//SExec executes the statement saved as name
func (tx *TX) SQLExec(query string, args ...interface{}) error {
	for name, value := range tx.dbome.vars {
		query = strings.Replace(query, name, value, -1)
	}
	_, err := tx.Exec(query, args...)
	return err
}

//SQuery executes the query statement saved as name
func (tx *TX) SQLQuery(query string, scannerName string, args ...interface{}) (Cursor, error) {
	for name, value := range tx.dbome.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	scanner, err := tx.dbome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

// SQueryFirst get the first result of the query statement saved as name
func (tx *TX) SQLQueryFirst(query string, scannerName string, args ...interface{}) (interface{}, error) {
	for name, value := range tx.dbome.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	scanner, err := tx.dbome.findScanner(scannerName)
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
