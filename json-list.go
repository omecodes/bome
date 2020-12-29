package bome

import (
	"context"
	"database/sql"
	"fmt"
)

type JSONList struct {
	*List
	*JsonValueHolder
	*Bome
	tableName string
}

func (l *JSONList) Table() string {
	return l.tableName
}

func (l *JSONList) Keys() []string {
	return []string{
		"ind",
	}
}

func (l *JSONList) Transaction(ctx context.Context) (context.Context, *JSONListTx, error) {
	tx := transaction(ctx)
	if tx == nil {
		tx, err := l.Bome.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONListTx{
			tableName: l.tableName,
			tx:        tx,
		}, nil
	}

	return ctx, &JSONListTx{
		tableName: l.tableName,
		tx:        tx.clone(l.Bome),
	}, nil
}

func (l *JSONList) BeginTransaction() (*JSONListTx, error) {
	tx, err := l.BeginTx()
	if err != nil {
		return nil, err
	}

	return &JSONListTx{
		tableName: l.tableName,
		tx:        tx,
	}, nil
}

func (l *JSONList) ContinueTransaction(tx *TX) *JSONListTx {
	return &JSONListTx{
		tableName: l.tableName,
		tx:        tx.clone(l.Bome),
	}
}

func (l *JSONList) Client() Client {
	return l.Bome
}

func (l *JSONList) EditAt(index int64, path string, sqlValue string) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where ind=?;", path, sqlValue)
	return l.Client().SQLExec(rawQuery, index)
}

func (l *JSONList) ExtractAt(index int64, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from $table$ where ind=?;", path)
	o, err := l.Client().SQLQueryFirst(rawQuery, StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

// NewJSONList creates a single table based list
// The table has two columns: an integer index and a json-string value
func NewJSONList(db *sql.DB, dialect string, tableName string) (*JSONList, error) {
	d := new(JSONList)
	d.List = new(List)
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
	d.List = &List{
		Bome: b,
	}

	d.SetTableName(escaped(tableName)).
		AddTableDefinition(
			"create table if not exists $table$ (ind bigint not null primary key $auto_increment$, value json not null);")
	return d, d.Init()
}
