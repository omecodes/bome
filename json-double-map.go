package bome

import (
	"context"
	"database/sql"
	"fmt"
)

type JSONDoubleMap struct {
	*Bome
	*DoubleMap
	*JsonValueHolder
	tableName string
	dialect   string
}

func (s *JSONDoubleMap) Table() string {
	return s.tableName
}

func (s *JSONDoubleMap) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (s *JSONDoubleMap) Transaction(ctx context.Context) (context.Context, *JSONDoubleMapTx, error) {
	tx := transaction(ctx)
	if tx == nil {
		tx, err := s.Bome.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONDoubleMapTx{
			tx:        tx,
			tableName: s.tableName,
		}, nil
	}

	return ctx, &JSONDoubleMapTx{
		tableName: s.tableName,
		tx:        tx.clone(s.Bome),
	}, nil
}

func (s *JSONDoubleMap) BeginTransaction() (*JSONDoubleMapTx, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &JSONDoubleMapTx{
		tableName: s.tableName,
		tx:        tx,
	}, nil
}

func (s *JSONDoubleMap) ContinueTransaction(tx *TX) *JSONDoubleMapTx {
	return &JSONDoubleMapTx{
		tableName: s.tableName,
		tx:        tx.clone(s.Bome),
	}
}

func (s *JSONDoubleMap) Client() Client {
	return s.Bome
}

func (s *JSONDoubleMap) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		normalizedJsonPath(path),
		value,
	)
	return s.Client().SQLExec(rawQuery, firstKey, secondKey)
}

func (s *JSONDoubleMap) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	o, err := s.Client().SQLQueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONDoubleMap creates MySQL wrapped DoubleMap
func NewJSONDoubleMap(db *sql.DB, dialect string, tableName string) (*JSONDoubleMap, error) {
	d := new(JSONDoubleMap)
	d.dialect = dialect
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
	d.JsonValueHolder = NewJsonValueHolder("value", b)
	d.DoubleMap = &DoubleMap{
		Bome: b,
	}

	d.SetTableName(escaped(tableName)).
		AddTableDefinition("create table if not exists $table$ (first_key varchar(255) not null, second_key varchar(255) not null, value json not null);")

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$table$", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
