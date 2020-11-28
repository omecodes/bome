package bome

import (
	"fmt"
	"log"
)

type JSONDoubleMapTx struct {
	tableName string
	tx        *TX
}

func (tx *JSONDoubleMapTx) Client() Client {
	return tx.tx
}

func (tx *JSONDoubleMapTx) Contains(firstKey, secondKey string) (bool, error) {
	query := "select 1 from " + tx.tableName + " where first_key=? and second_key=?;"
	o, err := tx.Client().SQLQueryFirst(
		query, BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (tx *JSONDoubleMapTx) Save(m *DoubleMapEntry) error {
	query := "insert into " + tx.tableName + " values (?, ?, ?);"
	if tx.Client().SQLExec(query, m.FirstKey, m.SecondKey, m.Value) != nil {
		query = "update " + tx.tableName + "  set value=? where first_key=? and second_key=?;"
		return tx.Client().SQLExec(query, m.Value, m.FirstKey, m.SecondKey)
	}
	return nil
}

func (tx *JSONDoubleMapTx) Get(firstKey, secondKey string) (string, error) {
	query := "select value from " + tx.tableName + " where first_key=? and second_key=?;"
	o, err := tx.Client().SQLQueryFirst(query, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONDoubleMapTx) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	query := "select second_key, value from " + tx.tableName + "  where first_key=? limit ?, ?;"
	c, err := tx.Client().SQLQuery(query, MapEntryScanner, key, offset, count)
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
		o, err := c.Next()
		if err != nil {
			return nil, err
		}

		entries = append(entries, o.(*MapEntry))
	}
	return entries, nil
}

func (tx *JSONDoubleMapTx) RangeMatchingSecondKey(key string, offset, count int) ([]*MapEntry, error) {
	query := "select first_key, value from " + tx.tableName + " where second_key=? limit ?, ?;"
	c, err := tx.Client().SQLQuery(query, MapEntryScanner, key, offset, count)
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
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*MapEntry))
	}
	return entries, nil
}

func (tx *JSONDoubleMapTx) Range(offset, count int) ([]*DoubleMapEntry, error) {
	query := "select * from " + tx.tableName + "  limit ?, ?;"
	c, err := tx.Client().SQLQuery(query, DoubleMapEntryScanner, offset, count)
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
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*DoubleMapEntry))
	}
	return entries, nil
}

func (tx *JSONDoubleMapTx) GetForFirst(firstKey string) (Cursor, error) {
	query := "select second_key, value from " + tx.tableName + " where first_key=?;"
	return tx.Client().SQLQuery(query, MapEntryScanner, firstKey)
}

func (tx *JSONDoubleMapTx) GetForSecond(secondKey string) (Cursor, error) {
	query := "select first_key, value from " + tx.tableName + " where second_key=?;"
	return tx.Client().SQLQuery(query, MapEntryScanner, secondKey)
}

func (tx *JSONDoubleMapTx) Count() (int, error) {
	query := "select count(*) from " + tx.tableName
	o, err := tx.Client().SQLQueryFirst(query, IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *JSONDoubleMapTx) CountForFirstKey(key string) (int, error) {
	query := "select count(*) from " + tx.tableName + " where first_key=?;"
	o, err := tx.Client().SQLQueryFirst(query, IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *JSONDoubleMapTx) CountForSecondKey(key string) (int, error) {
	query := "select count(*) from " + tx.tableName + " where second_key=?;"
	o, err := tx.Client().SQLQueryFirst(query, IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *JSONDoubleMapTx) Size(firstKey string, secondKey string) (int, error) {
	query := "select coalesce(length(value), 0) from " + tx.tableName + " where first_key=? and second_key=?;"
	o, err := tx.Client().SQLQueryFirst(query, IntScanner, firstKey, secondKey)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *JSONDoubleMapTx) TotalSize() (int64, error) {
	query := "select coalesce(sum(length(value)), 0) from " + tx.tableName
	o, err := tx.Client().SQLQueryFirst(query, IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *JSONDoubleMapTx) GetAll() (Cursor, error) {
	query := "select * from " + tx.tableName
	return tx.Client().SQLQuery(query, DoubleMapEntryScanner)
}

func (tx *JSONDoubleMapTx) Delete(firstKey, secondKey string) error {
	query := "delete from " + tx.tableName + "  where first_key=? and second_key=?;"
	return tx.Client().SQLExec(query, firstKey, secondKey)
}

func (tx *JSONDoubleMapTx) DeleteAllMatchingFirstKey(firstKey string) error {
	query := "delete from " + tx.tableName + " where first_key=?;"
	return tx.Client().SQLExec(query, firstKey)
}

func (tx *JSONDoubleMapTx) DeleteAllMatchingSecondKey(secondKey string) error {
	query := "delete from " + tx.tableName + " where second_key=?;"
	return tx.Client().SQLExec(query, secondKey)
}

func (tx *JSONDoubleMapTx) Clear() error {
	query := "delete from " + tx.tableName
	return tx.Client().SQLExec(query)
}

func (tx *JSONDoubleMapTx) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update %s set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		tx.tableName,
		normalizedJsonPath(path),
		value,
	)
	return tx.Client().SQLExec(rawQuery, firstKey, secondKey)
}

func (tx *JSONDoubleMapTx) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from %s where first_key=? and second_key=?;", path, tx.tableName)
	o, err := tx.Client().SQLQueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONDoubleMapTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update %s set value=json_set(value, '%s', %s);",
		tx.tableName,
		normalizedJsonPath(path),
		ex.eval(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONDoubleMapTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update %s set value=json_insert(value, '%s', %s) where %s",
		tx.tableName,
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONDoubleMapTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from %s where %s;",
		path,
		tx.tableName,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONDoubleMapTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from %s where %s;",
		tx.tableName,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONDoubleMapTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from %s where %s limit ?, ?;",
		tx.tableName,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName, offset, count)
}

func (tx *JSONDoubleMapTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *JSONDoubleMapTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *JSONDoubleMapTx) TX() *TX {
	return tx.tx
}
