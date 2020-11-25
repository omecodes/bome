package bome

import (
	"database/sql"
	"fmt"
)

// JSONDoubleMap is a convenience for double mapping persistent store
type JSONDoubleMap interface {
	DoubleMap
	JsonValueHolder
	EditAt(firstKey, secondKey string, path string, data string) error
	ExtractAt(firstKey, secondKey string, path string) (string, error)
}

type jsonDoubleMap struct {
	*Bome
	*doubleMap
	JsonValueHolder
	tableName string
	dialect   string
}

func (s *jsonDoubleMap) BeginTransaction() (JSONDoubleMapTransaction, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &txJsonDoubleMap{
		jsonDoubleMap: s,
		tx:            tx,
	}, nil
}

func (s *jsonDoubleMap) ContinueTransaction(tx *TX) JSONDoubleMapTransaction {
	return &txJsonDoubleMap{
		jsonDoubleMap: s,
		tx:            tx,
	}
}

func (s *jsonDoubleMap) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update %s set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		s.tableName,
		normalizedJsonPath(path),
		value,
	)
	return s.Client().SQLExec(rawQuery, firstKey, secondKey)
}

func (s *jsonDoubleMap) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from %s where first_key=? and second_key=?;", path, s.tableName)
	o, err := s.Client().SQLQueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONDoubleMap creates MySQL wrapped DoubleMap
func NewJSONDoubleMap(db *sql.DB, dialect string, tableName string) (JSONDoubleMap, error) {
	d := new(jsonDoubleMap)
	d.doubleMap = new(doubleMap)
	d.tableName = tableName
	d.dialect = dialect

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
	d.doubleMap = &doubleMap{
		Bome: b,
	}

	d.SetTablePrefix(tableName).
		AddTableDefinition("create table if not exists $prefix$ (first_key varchar(255) not null, second_key varchar(255) not null, value json not null);")

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$prefix$", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
