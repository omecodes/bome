package bome

import (
	"fmt"
	"log"
)

type JSONDoubleMapTx struct {
	tx *TX
}

func (tx *JSONDoubleMapTx) Client() Client {
	return tx.tx
}

func (tx *JSONDoubleMapTx) Save(m *DoubleMapEntry) error {
	if tx.Client().SQLExec("insert into $table$ values (?, ?, ?);", m.FirstKey, m.SecondKey, m.Value) != nil {
		return tx.Client().SQLExec("update $table$ set value=? where first_key=? and second_key=?;", m.Value, m.FirstKey, m.SecondKey)
	}
	return nil
}

func (tx *JSONDoubleMapTx) Get(firstKey, secondKey string) (string, error) {
	o, err := tx.Client().SQLQueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONDoubleMapTx) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := tx.Client().SQLQuery("select second_key, value from $table$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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
	c, err := tx.Client().SQLQuery("select first_key, value from $table$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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
	c, err := tx.Client().SQLQuery("select * from $table$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
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
	return tx.Client().SQLQuery("select second_key, value from $table$ where first_key=?;", MapEntryScanner, firstKey)
}

func (tx *JSONDoubleMapTx) GetForSecond(secondKey string) (Cursor, error) {
	return tx.Client().SQLQuery("select first_key, value from $table$ where second_key=?;", MapEntryScanner, secondKey)
}

func (tx *JSONDoubleMapTx) GetAll() (Cursor, error) {
	return tx.Client().SQLQuery("select * from $table$;", DoubleMapEntryScanner)
}

func (tx *JSONDoubleMapTx) Delete(firstKey, secondKey string) error {
	return tx.Client().SQLExec("delete from $table$ where first_key=? and second_key=?;", firstKey, secondKey)
}

func (tx *JSONDoubleMapTx) DeleteAllMatchingFirstKey(firstKey string) error {
	return tx.Client().SQLExec("delete from $table$ where first_key=?;", firstKey)
}

func (tx *JSONDoubleMapTx) DeleteAllMatchingSecondKey(secondKey string) error {
	return tx.Client().SQLExec("delete from $table$ where second_key=?;", secondKey)
}

func (tx *JSONDoubleMapTx) Clear() error {
	return tx.Client().SQLExec("delete from $table$;")
}

func (tx *JSONDoubleMapTx) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		normalizedJsonPath(path),
		value,
	)
	return tx.Client().SQLExec(rawQuery, firstKey, secondKey)
}

func (tx *JSONDoubleMapTx) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	o, err := tx.Client().SQLQueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONDoubleMapTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s);",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONDoubleMapTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_insert(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONDoubleMapTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where %s;",
		path,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONDoubleMapTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONDoubleMapTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
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
