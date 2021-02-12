package bome

import (
	"context"
	"fmt"
)

type JSONMap struct {
	*Map
	*DB
	*JsonValueHolder
	tx        *TX
	dialect   string
	tableName string
}

func (m *JSONMap) Table() string {
	return m.tableName
}

func (m *JSONMap) Keys() []string {
	return []string{
		"name",
	}
}

func (m *JSONMap) Transaction(ctx context.Context) (context.Context, *JSONMap, error) {
	if m.tx != nil {
		tx := transaction(ctx)
		if tx == nil {
			return contextWithTransaction(ctx, m.tx), m, nil
		}
		return ctx, m, nil
	}

	tx := transaction(ctx)
	if tx == nil {
		tx, err := m.DB.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONMap{
			JsonValueHolder: &JsonValueHolder{
				tx:      m.tx,
				dialect: m.dialect,
			},
			Map: &Map{
				tableName: m.tableName,
				tx:        tx,
				dialect:   m.dialect,
			},
			tableName: m.tableName,
			tx:        tx,
			dialect:   m.dialect,
		}, nil
	}

	return ctx, &JSONMap{
		JsonValueHolder: &JsonValueHolder{
			tx:      m.tx,
			dialect: m.dialect,
		},
		Map: &Map{
			tableName: m.tableName,
			tx:        tx,
			dialect:   m.dialect,
		},
		tableName: m.tableName,
		tx:        tx,
		dialect:   m.dialect,
	}, nil
}

func (m *JSONMap) SwitchToTransactionMode(tx *TX) *JSONMap {
	return &JSONMap{
		JsonValueHolder: &JsonValueHolder{
			tx:      m.tx,
			dialect: m.dialect,
		},
		Map: &Map{
			tableName: m.tableName,
			tx:        tx,
			dialect:   m.dialect,
		},
		tableName: m.tableName,
		tx:        tx,
	}
}

func (m *JSONMap) Client() Client {
	if m.tx != nil {
		return m.tx
	}
	return m.DB
}

func (m *JSONMap) Count() (int, error) {
	o, err := m.Client().QueryFirst("select count(*) from $table$;", IntScanner)
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
	return m.Client().Exec(rawQuery).Error
}

func (m *JSONMap) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return m.Client().Exec(rawQuery).Error
}

func (m *JSONMap) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	var rawQuery string

	if m.dialect == SQLite3 {
		rawQuery = fmt.Sprintf("select json_extract(value, '%s') from $table$ where %s;",
			path,
			condition.sql(),
		)
	} else {
		rawQuery = fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where %s;",
			path,
			condition.sql(),
		)
	}

	return m.Client().Query(rawQuery, scannerName)
}

func (m *JSONMap) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	return m.Client().Query(rawQuery, scannerName)
}

func (m *JSONMap) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	return m.Client().Query(rawQuery, scannerName, offset, count)
}

func (m *JSONMap) EditAt(key string, path string, ex Expression) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', %s) where name=?;",
		normalizedJsonPath(path),
		ex.eval())
	return m.Client().Exec(rawQuery, key).Error
}

func (m *JSONMap) ExtractAt(key string, path string) (string, error) {
	var rawQuery string

	if m.dialect == SQLite3 {
		rawQuery = fmt.Sprintf("select json_extract(value, '%s') from $table$ where name=?;", path)
	} else {
		rawQuery = fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where name=?;", path)
	}

	o, err := m.Client().QueryFirst(rawQuery, StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}
