package bome

import (
	"database/sql"
	"errors"
)

type TX struct {
	db *DB
	*sql.Tx
}

func (tx *TX) SExec(saved string, args ...interface{}) error {
	stmt, found := tx.db.registeredStatements[saved]
	if !found {
		return errors.New("no statement found")
	}
	_, err := tx.Exec(stmt, args...)
	return err
}

func (tx *TX) SQuery(saved string, scannerName string, args ...interface{}) (Cursor, error) {
	stmt, found := tx.db.registeredStatements[saved]
	if !found {
		return nil, errors.New("no statement found")
	}

	rows, err := tx.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	scanner, err := tx.db.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

func (tx *TX) SQueryFirst(saved string, scannerName string, args ...interface{}) (interface{}, error) {
	stmt, found := tx.db.registeredStatements[saved]
	if !found {
		return nil, errors.New("no statement found")
	}

	rows, err := tx.Query(stmt, args...)
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

func (tx *TX) Commit() error {
	return tx.Tx.Commit()
}

func (tx *TX) Rollback() error {
	return tx.Tx.Rollback()
}
