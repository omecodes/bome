package bome

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/omecodes/errors"
)

type Map struct {
	*DB
	*JsonValueHolder
	tx        *TX
	dialect   string
	tableName string
}

func (m *Map) Table() string {
	return m.tableName
}

func (m *Map) Keys() []string {
	return []string{
		"name",
	}
}

func (m *Map) Transaction(ctx context.Context) (context.Context, *Map, error) {
	tx := transaction(ctx)
	if tx == nil {
		if m.tx != nil {
			return contextWithTransaction(ctx, m.tx.New(m.DB)), m, nil
		}

		var err error
		tx, err = m.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &Map{
			JsonValueHolder: &JsonValueHolder{
				dialect: m.dialect,
				tx:      tx,
			},
			tableName: m.tableName,
			tx:        tx,
			dialect:   m.dialect,
		}, nil
	}

	if m.tx != nil {
		if m.tx.db.sqlDb != tx.db.sqlDb {
			newCtx := ContextWithCommitActions(ctx, tx.Commit)
			newCtx = ContextWithRollbackActions(newCtx, tx.Rollback)
			return contextWithTransaction(newCtx, m.tx.New(m.DB)), m, nil
		}
		return ctx, m, nil
	}

	tx = tx.New(m.DB)
	newCtx := contextWithTransaction(ctx, tx)
	return newCtx, &Map{
		JsonValueHolder: &JsonValueHolder{
			dialect: m.dialect,
			tx:      tx,
		},
		tableName: m.tableName,
		tx:        tx,
		dialect:   m.dialect,
	}, nil
}

func (m *Map) Commit() error {
	if m.tx != nil {
		return m.tx.Commit()
	}
	return nil
}

func (m *Map) Rollback() error {
	if m.tx != nil {
		return m.tx.Rollback()
	}
	return nil
}

func (m *Map) Client() Client {
	if m.tx != nil {
		return m.tx
	}
	return m.DB
}

func (m *Map) Save(key string, o interface{}, opts SaveOptions) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}

	err = m.Client().Exec("insert into $table$ values (?, ?);", key, string(data)).Error
	if opts.UpdateExisting && isPrimaryKeyConstraintError(err) {
		return m.Client().Exec("update $table$ set value=? where name=?;", string(data), key).Error
	}
	return err
}

func (m *Map) SaveRaw(key string, value string, opts SaveOptions) error {
	err := m.Client().Exec("insert into $table$ values (?, ?);", key, value).Error
	if opts.UpdateExisting && isPrimaryKeyConstraintError(err) {
		return m.Client().Exec("update $table$ set value=? where name=?;", value, key).Error
	}
	return err
}

func (m *Map) Get(key string, o interface{}) error {
	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}

	value, err := m.Client().QueryFirst("select value from $table$ where name=?;", StringScanner, key)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(value.(string)), o)
}

func (m *Map) GetRaw(key string) (string, error) {
	value, err := m.Client().QueryFirst("select value from $table$ where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (m *Map) Size(key string) (int64, error) {
	o, err := m.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (m *Map) TotalSize() (int64, error) {
	o, err := m.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (m *Map) Contains(key string) (bool, error) {
	res, err := m.Client().QueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (m *Map) Range(offset, count int) ([]*MapEntry, error) {
	c, err := m.Client().Query("select * from $table$ limit ?, ?;", MapEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*MapEntry
	for c.HasNext() {
		o, err := c.Entry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*MapEntry))
	}
	return entries, nil
}

func (m *Map) Delete(key string) error {
	return m.Client().Exec("delete from $table$ where name=?;", key).Error
}

func (m *Map) List() (Cursor, error) {
	return m.Client().Query("select * from $table$;", MapEntryScanner)
}

func (m *Map) Clear() error {
	return m.Client().Exec("delete from $table$;").Error
}

func (m *Map) Close() error {
	return m.DB.sqlDb.Close()
}

func (m *Map) Count() (int64, error) {
	o, err := m.Client().QueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (m *Map) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s);",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return m.Client().Exec(rawQuery).Error
}

func (m *Map) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return m.Client().Exec(rawQuery).Error
}

func (m *Map) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
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

func (m *Map) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	return m.Client().Query(rawQuery, scannerName, offset, count)
}

func (m *Map) EditAt(key string, path string, ex Expression) error {
	ex.setDialect(m.dialect)
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', %s) where name=?;",
		normalizedJsonPath(path),
		ex.eval())
	return m.Client().Exec(rawQuery, key).Error
}

func (m *Map) ExtractAt(key string, path string) (string, error) {
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
