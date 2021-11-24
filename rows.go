package bome

import (
	"database/sql"
	"encoding/json"
	"reflect"
)

// Cursor is a convenience for generic objects cursor
type Cursor interface {
	HasNext() bool
	Next() (interface{}, error)
	Close() error
}

// ObjectCursor is a convenience for generic objects cursor
type ObjectCursor interface {
	HasNext() bool
	Read(o interface{}) error
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

type objectCursor struct {
	scanner Scanner
	rows    *sql.Rows
}

func (c *objectCursor) HasNext() bool {
	return c.rows.Next()
}

func (c *objectCursor) Read(o interface{}) error {
	var value string
	err := c.rows.Scan(&value)
	if err != nil {
		return err
	}

	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}

	return json.Unmarshal([]byte(value), o)
}

func (c *objectCursor) Close() error {
	return c.rows.Close()
}
