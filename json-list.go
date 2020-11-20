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

type jsonListDB struct {
	*listDB
	JsonValueHolder
	*Bome
	tableName string
}

func (l *jsonListDB) EditAt(index int64, path string, sqlValue string) error {
	rawQuery := fmt.Sprintf(
		"update %s set value=json_set(value, '%s', %s) where ind=?;", l.tableName, path, sqlValue)
	return l.RawExec(rawQuery, index).Error
}

func (l *jsonListDB) ExtractAt(index int64, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from %s where ind=?;", path, l.tableName)
	o, err := l.RawQueryFirst(rawQuery, StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONList creates a single table based list
// The table has two columns: an integer index and a json-string value
func NewJSONList(db *sql.DB, dialect string, tableName string) (JSONList, error) {
	d := new(jsonListDB)
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
		AddTableDefinition("create table if not exists $prefix$ (ind integer not null primary key $auto_increment$, value json not null);").
		AddStatement("insert", "insert into $prefix$ values (?, ?);").
		AddStatement("append", "insert into $prefix$ (value) values (?);").
		AddStatement("select", "select * from $prefix$ where ind=?;").
		AddStatement("select_min_index", "select min(ind) from $prefix$;").
		AddStatement("select_max_index", "select max(ind) from $prefix$;").
		AddStatement("select_count", "select count(ind) from $prefix$;").
		AddStatement("select_from", "select * from $prefix$ where ind>? order by ind;").
		AddStatement("delete_by_seq", "delete from $prefix$ where ind=?;").
		AddStatement("clear", "delete from $prefix$;")
	return d, d.Init()
}
