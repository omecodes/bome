package bome

import (
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/mattn/go-sqlite3"
)

var TableNotFound = errors.New("bome: table not found")
var IndexNotFound = errors.New("bome: index not found")
var InitError = errors.New("bome: init error")
var DialectNotSupported = errors.New("bome: unsupported dialect")
var StatementNotFound = errors.New("statement not found")
var ScannerNotFound = errors.New("scanner not found")
var EntryNotFound = errors.New("not found")
var TransactionNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	return errors.Is(err, EntryNotFound)
}

func IsTransactionNotFound(err error) bool {
	return errors.Is(err, TransactionNotFound)
}

func IsPrimaryKeyConstraintError(err error) bool {
	if me, ok := err.(*mysql.MySQLError); ok {
		return me.Number == 1062

	} else if se, ok := err.(*sqlite3.Error); ok {
		return se.ExtendedCode == sqlite3.ErrConstraintPrimaryKey
	}
	return false
}

func IsForeignKeyConstraintError(err error) bool {
	if me, ok := err.(*mysql.MySQLError); ok {
		return me.Number == 1216

	} else if se, ok := err.(*sqlite3.Error); ok {
		return se.ExtendedCode == sqlite3.ErrConstraintForeignKey
	}
	return false
}
