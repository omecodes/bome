package bome

type Client interface {
	Exec(query string, args ...interface{}) Result
	Query(query string, scannerName string, args ...interface{}) (Cursor, error)
	QueryFirst(query string, scannerName string, args ...interface{}) (interface{}, error)
}
