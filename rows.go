package bome

import "database/sql"

// Cursor is a convenience for generic objects cursor
type Cursor interface {
	HasNext() bool
	Next() (interface{}, error)
	Close() error
}

// Row is a convenience for generic row
type Row interface {
	Scan(dest ...interface{}) error
}

// Scanner is a convenience for generic row scanner
type Scanner interface {
	ScanRow(row Row) (interface{}, error)
}

func newCursor(rows *sql.Rows, scanner Scanner) Cursor {
	return &cursor{
		scanner: scanner,
		rows:    rows,
	}
}

type cursor struct {
	scanner Scanner
	rows    *sql.Rows
}

func (c *cursor) HasNext() bool {
	return c.rows.Next()
}

func (c *cursor) Next() (interface{}, error) {
	return c.scanner.ScanRow(c.rows)
}

func (c *cursor) Close() error {
	return c.rows.Close()
}
