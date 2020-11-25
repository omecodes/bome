package bome

import (
	"database/sql"
	"fmt"
)

// JSONList is a convenience for persistence list
type JSONList interface {
	List
	EditAt(index int64, path string, sqlValue string) error
	ExtractAt(index int64, path string) (string, error)
}

type jsonList struct {
	*listDB
	JsonValueHolder
	*Bome
	tableName string
}

func (l *jsonList) BeginTransaction() (JsonListTransaction, error) {
	tx, err := l.BeginTx()
	if err != nil {
		return nil, err
	}
	return &txJsonList{
		jsonList: l,
		tx:       tx,
	}, nil
}

func (l *jsonList) ContinueTransaction(tx *TX) JsonListTransaction {
	return &txJsonList{
		jsonList: l,
		tx:       tx,
	}
}

func (l *jsonList) Client() Client {
	return l.Bome
}

func (l *jsonList) EditAt(index int64, path string, sqlValue string) error {
	rawQuery := fmt.Sprintf(
		"update %s set value=json_set(value, '%s', %s) where ind=?;", l.tableName, path, sqlValue)
	return l.Client().SQLExec(rawQuery, index)
}

func (l *jsonList) ExtractAt(index int64, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from %s where ind=?;", path, l.tableName)
	o, err := l.Client().SQLQueryFirst(rawQuery, StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONList creates a single table based list
// The table has two columns: an integer index and a json-string value
func NewJSONList(db *sql.DB, dialect string, tableName string) (JSONList, error) {
	d := new(jsonList)
	d.tableName = escaped(tableName)
	d.listDB = new(listDB)

	var err error
	var b *Bome
	if dialect == SQLite3 {
		b, err = NewLite(db)
	} else if dialect == MySQL {
		b, err = New(db)
	} else {
		return nil, DialectNotSupported
	}

	if err != nil {
		return nil, err
	}

	d.Bome = b
	d.JsonValueHolder = NewJsonValueHolder(d.tableName, "value", b)
	d.listDB = &listDB{
		Bome: b,
	}

	d.SetTablePrefix(tableName).
		AddTableDefinition("create table if not exists $prefix$ (ind integer not null primary key $auto_increment$, value json not null);")
	return d, d.Init()
}
