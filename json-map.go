package bome

import (
	"database/sql"
	"fmt"
	"strings"
)

// JSONMap is a convenience for persistent string to string dict
type JSONMap interface {
	Map
	JsonValueHolder
	EditAt(key string, path string, ex Expression) error
	ExtractAt(key string, path string) (string, error)
}

type jsonMap struct {
	*dict
	*Bome
	JsonValueHolder

	tableName string
}

func (m *jsonMap) EditAt(key string, path string, ex Expression) error {
	rawQuery := fmt.Sprintf("update %s set value=json_set(value, '%s', %s) where name=?;",
		m.tableName,
		normalizedJsonPath(path),
		ex.eval(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", "value", -1)
	return m.RawExec(rawQuery, key).Error
}

func (m *jsonMap) ExtractAt(key string, path string) (string, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from %s where name=?;", path, m.tableName)
	o, err := m.RawQueryFirst(rawQuery, StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONMap creates a table based key-value map
// The table has two columns: an string key and a json-string value
func NewJSONMap(db *sql.DB, dialect string, tableName string) (JSONMap, error) {
	d := new(jsonMap)
	d.tableName = escaped(tableName)
	d.dict = new(dict)

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
	d.dict = &dict{Bome: b}

	d.SetTablePrefix(d.tableName).
		AddTableDefinition("create table if not exists $prefix$ (name varchar(255) not null primary key, value json not null);").
		AddStatement("insert", "insert into $prefix$ values (?, ?);").
		AddStatement("update", "update $prefix$ set value=? where name=?;").
		AddStatement("select", "select value from $prefix$ where name=?;").
		AddStatement("select_all", "select * from $prefix$;").
		AddStatement("contains", "select 1 from $prefix$ where name=?;").
		AddStatement("delete", "delete from $prefix$ where name=?;").
		AddStatement("clear", "delete from $prefix$;")
	return d, d.Init()
}
