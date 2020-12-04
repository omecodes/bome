package bome

import "errors"

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
