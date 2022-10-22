package bome

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type DMap struct {
	*DB
	*JsonValueHolder
	tx        *TX
	tableName string
	dialect   string
}

func (s *DMap) Table() string {
	return s.tableName
}

func (s *DMap) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (s *DMap) Transaction(ctx context.Context) (context.Context, *DMap, error) {
	tx := transaction(ctx)
	if tx == nil {
		if s.tx != nil {
			return contextWithTransaction(ctx, s.tx.New(s.DB)), s, nil
		}

		var err error
		tx, err = s.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &DMap{
			JsonValueHolder: &JsonValueHolder{
				dialect: s.dialect,
				tx:      tx,
			},
			tableName: s.tableName,
			tx:        tx,
			dialect:   s.dialect,
		}, nil
	}

	if s.tx != nil {
		if s.tx.db.sqlDb != tx.db.sqlDb {
			newCtx := ContextWithCommitActions(ctx, tx.Commit)
			newCtx = ContextWithRollbackActions(newCtx, tx.Rollback)
			return contextWithTransaction(newCtx, s.tx.New(s.DB)), s, nil
		}
		return ctx, s, nil
	}

	tx = tx.New(s.DB)
	newCtx := contextWithTransaction(ctx, tx)
	return newCtx, &DMap{
		JsonValueHolder: &JsonValueHolder{
			dialect: s.dialect,
			tx:      tx,
		},
		tableName: s.tableName,
		tx:        tx,
		dialect:   s.dialect,
	}, nil
}

func (s *DMap) Commit() error {
	if s.tx != nil {
		return s.tx.Commit()
	}
	return nil
}

func (s *DMap) Rollback() error {
	if s.tx != nil {
		return s.tx.Rollback()
	}
	return nil
}

func (s *DMap) Client() Client {
	if s.tx != nil {
		return s.tx
	}
	return s.DB
}

func (s *DMap) Contains(key1, key2 string) (bool, error) {
	o, err := s.Client().QueryFirst("select 1 from $table$ where first_key=? and second_key=?;", BoolScanner, key1, key2)
	return o.(bool), err
}

func (s *DMap) Count() (int64, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DMap) CountForFirstKey(key string) (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$ where first_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DMap) CountForSecondKey(key string) (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$ where second_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DMap) Size(key1 string, key2 string) (int64, error) {
	o, err := s.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where first_key=? and second_key=?;", IntScanner, key1, key2)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DMap) TotalSize() (int64, error) {
	o, err := s.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DMap) Save(key1, key2 string, value string, opts SaveOptions) error {
	err := s.Client().Exec("insert into $table$ values (?, ?, ?);", key1, key2, value).Error
	if err != nil && isPrimaryKeyConstraintError(err) && opts.UpdateExisting {
		return s.Client().Exec("update $table$ set value=? where first_key=? and second_key=?;", value, key1, key2).Error
	}
	return err
}

func (s *DMap) Read(key1, key2 string, o interface{}) error {
	res, err := s.Client().QueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, key1, key2)
	if err != nil {
		return err
	}

	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}
	return json.Unmarshal([]byte(res.(string)), o)
}

func (s *DMap) ReadRaw(key1, key2 string) (string, error) {
	o, err := s.Client().QueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, key1, key2)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *DMap) RangeByFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().Query("select second_key, value from $table$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *DMap) RangeBySecondKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().Query("select first_key, value from $table$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *DMap) Range(offset, count int) ([]*DoubleMapEntry, error) {
	c, err := s.Client().Query("select * from $table$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()
	var entries []*DoubleMapEntry

	for c.HasNext() {
		o, err := c.Entry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*DoubleMapEntry))
	}
	return entries, nil
}

func (s *DMap) GetForFirst(key1 string) (Cursor, error) {
	return s.Client().Query("select second_key, value from $table$ where first_key=?;", MapEntryScanner, key1)
}

func (s *DMap) GetForSecond(key2 string) (Cursor, error) {
	return s.Client().Query("select first_key, value from $table$ where second_key=?;", MapEntryScanner, key2)
}

func (s *DMap) GetAll() (Cursor, error) {
	return s.Client().Query("select * from $table$;", DoubleMapEntryScanner)
}

func (s *DMap) AllByFirstKey(key string, where BoolExpr) (Cursor, error) {
	where.setDialect(s.dialect)
	rawQuery := fmt.Sprintf("select %s from $table$ where first_key=? and %s;",
		s.field,
		where.sql(),
	)
	return s.Client().Query(rawQuery, StringScanner, key)
}

func (s *DMap) AllBySecondKey(key string, where BoolExpr) (Cursor, error) {
	where.setDialect(s.dialect)
	rawQuery := fmt.Sprintf("select %s from $table$ where second_key=? and %s;",
		s.field,
		where.sql(),
	)
	return s.Client().Query(rawQuery, StringScanner, key)
}

func (s *DMap) Delete(key1, key2 string) error {
	return s.Client().Exec("delete from $table$ where first_key=? and second_key=?;", key1, key2).Error
}

func (s *DMap) DeleteAllByFirstKey(key1 string) error {
	return s.Client().Exec("delete from $table$ where first_key=?;", key1).Error
}

func (s *DMap) DeleteByFirstKey(key string, where BoolExpr) error {
	query := fmt.Sprintf("delete from $table$ where first_key=? and %s;", where.sql())
	return s.Client().Exec(query, key).Error
}

func (s *DMap) DeleteAllBySecondKey(key2 string) error {
	return s.Client().Exec("delete from $table$ where second_key=?;", key2).Error
}

func (s *DMap) DeleteByDeleteAllBySecondKey(key string, where BoolExpr) error {
	query := fmt.Sprintf("delete from $table$ where second_key=? and %s;", where.sql())
	return s.Client().Exec(query, key).Error
}

func (s *DMap) Edit(key1, key2 string, path string, ex Expression) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return s.Client().Exec(rawQuery, key1, key2).Error
}

func (s *DMap) String(key1, key2 string, path string) (string, error) {
	var rawQuery string

	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := s.Client().QueryFirst(rawQuery, StringScanner, key1, key2)
	if err != nil {
		return "", err
	}
	value := o.(string)
	if s.dialect == SQLite3 {
		value = strings.Trim(value, "'")
	}
	return value, nil
}

func (s *DMap) Float(key1, key2 string, path string) (float64, error) {
	var rawQuery string

	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := s.Client().QueryFirst(rawQuery, StringScanner, key1, key2)
	if err != nil {
		return 0., err
	}
	return o.(float64), nil
}

func (s *DMap) Int(key1, key2 string, path string) (int64, error) {
	var rawQuery string

	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := s.Client().QueryFirst(rawQuery, IntScanner, key1, key2)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DMap) Bool(key1, key2 string, path string) (bool, error) {
	var rawQuery string

	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := s.Client().QueryFirst(rawQuery, BoolScanner, key1, key2)
	if err != nil {
		return false, err
	}
	return o.(bool), nil
}

func (s *DMap) Clear() error {
	return s.Client().Exec("delete from $table$;").Error
}

func (s *DMap) Close() error {
	return s.DB.sqlDb.Close()
}
