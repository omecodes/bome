package bome

type Client interface {
	SQLExec(query string, args ...interface{}) error
	SQLQuery(query string, scannerName string, args ...interface{}) (Cursor, error)
	SQLQueryFirst(query string, scannerName string, args ...interface{}) (interface{}, error)
}
