package bome

type Client interface {
	Exec(query string, args ...interface{}) Result
	Query(query string, scannerName string, args ...interface{}) (Cursor, error)
	QueryObjects(query string, args ...interface{}) (ObjectCursor, error)
	QueryFirst(query string, scannerName string, args ...interface{}) (interface{}, error)
}
