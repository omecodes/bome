package bome

import (
	"database/sql"
	"fmt"
)

type JSONMap struct {
	*Map
	*Bome
	tableName string
}

func (m *JSONMap) BeginTransaction() (*JSONMapTx, error) {
	tx, err := m.BeginTx()
	if err != nil {
		return nil, err
	}
	return &JSONMapTx{
		tableName: m.tableName,
		tx:        tx,
	}, nil
}

func (m *JSONMap) ContinueTransaction(tx *TX) *JSONMapTx {
	return &JSONMapTx{
		tableName: m.tableName,
		tx:        tx,
	}
}

func (m *JSONMap) Client() Client {
	return m.Bome
}

func (m *JSONMap) Count() (int, error) {
	o, err := m.Client().SQLQueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (m *JSONMap) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s);",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return m.Client().SQLExec(rawQuery)
}

func (m *JSONMap) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_insert(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return m.Client().SQLExec(rawQuery)
}

func (m *JSONMap) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where %s;",
		path,
		condition.sql(),
	)
	return m.Client().SQLQuery(rawQuery, scannerName)
}

func (m *JSONMap) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	return m.Client().SQLQuery(rawQuery, scannerName)
}

func (m *JSONMap) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	return m.Client().SQLQuery(rawQuery, scannerName, offset, count)
}

func (m *JSONMap) EditAt(key string, path string, ex Expression) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', %s) where name=?;",
		normalizedJsonPath(path),
		ex.eval())
	return m.Client().SQLExec(rawQuery, key)
}

func (m *JSONMap) ExtractAt(key string, path string) (string, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where name=?;", path)
	o, err := m.Client().SQLQueryFirst(rawQuery, StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONMap creates a table based key-value map
// The table has two columns: an string key and a json-string value
func NewJSONMap(db *sql.DB, dialect string, tableName string) (*JSONMap, error) {
	d := new(JSONMap)
	d.tableName = tableName
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
	d.Map = &Map{Bome: b}

	d.SetTableName(escaped(tableName)).
		AddTableDefinition("create table if not exists $table$ (name varchar(255) not null primary key, value json not null);")
	return d, d.Init()
}
