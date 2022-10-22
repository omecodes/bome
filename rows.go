package bome

import (
	"database/sql"
	"encoding/json"
	"reflect"

	"github.com/omecodes/errors"
)

// Cursor is a convenience for generic objects cursor.
type Cursor interface {
	HasNext() bool
	Entry() (interface{}, error)
	Value() (string, error)
	Read(o interface{}) error
	Close() error
}

// Row is a convenience for generic row.
type Row interface {
	Scan(dest ...interface{}) error
}

// Scanner is a convenience for generic row scanner.
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

func (c *cursor) Entry() (interface{}, error) {
	return c.scanner.ScanRow(c.rows)
}

func (c *cursor) Value() (string, error) {
	o, err := c.scanner.ScanRow(c.rows)
	if err != nil {
		return "", err
	}
	switch v := o.(type) {
	case string:
		return v, nil
	case *ListEntry:
		return v.Value, nil
	case *MapEntry:
		return v.Value, nil
	case *DoubleMapEntry:
		return v.Value, nil
	case *PairListEntry:
		return v.Value, nil
	}

	return "", errors.NotSupported()
}

func (c *cursor) Read(o interface{}) error {
	value, err := c.Value()
	if err != nil {
		return err
	}
	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}

	return json.Unmarshal([]byte(value), o)
}

func (c *cursor) Close() error {
	return c.rows.Close()
}
