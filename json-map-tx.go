package bome

import (
	"fmt"
	"log"
	"strings"
)

type JSONMapTx struct {
	tableName string
	tx        *TX
}

func (tx *JSONMapTx) Client() Client {
	return tx.tx
}

func (tx *JSONMapTx) Save(entry *MapEntry) error {
	err := tx.Client().SQLExec("insert into "+tx.tableName+" values (?, ?);", entry.Key, entry.Value)
	if err != nil {
		err = tx.Client().SQLExec("update "+tx.tableName+" set value=? where name=?;", entry.Value, entry.Key)
	}
	return err
}

func (tx *JSONMapTx) Get(key string) (string, error) {
	o, err := tx.Client().SQLQueryFirst("select value from "+tx.tableName+" where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONMapTx) Contains(key string) (bool, error) {
	res, err := tx.Client().SQLQueryFirst("select 1 from "+tx.tableName+" where name=?;", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (tx *JSONMapTx) Size(key string) (int, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(length(value), 0) from "+tx.tableName+" where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *JSONMapTx) TotalSize() (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(sum(length(value), 0) from "+tx.tableName+";", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *JSONMapTx) Range(offset, count int) ([]*MapEntry, error) {
	c, err := tx.Client().SQLQuery("select * from "+tx.tableName+" limit ?, ?;", MapEntryScanner, offset, count)
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

func (tx *JSONMapTx) Delete(key string) error {
	return tx.Client().SQLExec("delete from "+tx.tableName+" where name=?;", key)
}

func (tx *JSONMapTx) List() (Cursor, error) {
	return tx.Client().SQLQuery("select * from "+tx.tableName+";", MapEntryScanner)
}

func (tx *JSONMapTx) Clear() error {
	return tx.Client().SQLExec("delete from " + tx.tableName + ";")
}

func (tx *JSONMapTx) EditAt(key string, path string, ex Expression) error {
	rawQuery := fmt.Sprintf("update "+tx.tableName+" set value=json_set(value, '%s', %s) where name=?;",
		normalizedJsonPath(path),
		ex.eval())
	rawQuery = strings.Replace(rawQuery, "__value__", "value", -1)
	return tx.Client().SQLExec(rawQuery, key)
}

func (tx *JSONMapTx) ExtractAt(key string, path string) (string, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from "+tx.tableName+" where name=?;", path)
	o, err := tx.Client().SQLQueryFirst(rawQuery, StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONMapTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update "+tx.tableName+" set value=json_set(value, '%s', %s);",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONMapTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update "+tx.tableName+" set value=json_insert(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONMapTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from "+tx.tableName+" where %s;",
		path,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONMapTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from "+tx.tableName+" where %s;",
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONMapTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from "+tx.tableName+" where %s limit ?, ?;",
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName, offset, count)
}

func (tx *JSONMapTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *JSONMapTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *JSONMapTx) TX() *TX {
	return tx.tx
}
