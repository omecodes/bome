package bome

import (
	"database/sql"
	"errors"
)

// TX is a transaction token
type TX struct {
	dbome *Bome
	*sql.Tx
}

//SExec executes the statement saved as name
func (tx *TX) SExec(name string, args ...interface{}) error {
	stmt, found := tx.dbome.registeredStatements[name]
	if !found {
		return errors.New("no statement found")
	}
	_, err := tx.Exec(stmt, args...)
	return err
}

//SQuery executes the query statement saved as name
func (tx *TX) SQuery(name string, scannerName string, args ...interface{}) (Cursor, error) {
	stmt, found := tx.dbome.registeredStatements[name]
	if !found {
		return nil, errors.New("no statement found")
	}

	rows, err := tx.Query(stmt, args...)
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
func (tx *TX) SQueryFirst(name string, scannerName string, args ...interface{}) (interface{}, error) {
	stmt, found := tx.dbome.registeredStatements[name]
	if !found {
		return nil, errors.New("no statement found")
	}

	rows, err := tx.Query(stmt, args...)
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
