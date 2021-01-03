package bome

import (
	"fmt"
	"log"
)

type JSONListTx struct {
	tableName string
	tx        *TX
}

func (tx *JSONListTx) Table() string {
	return tx.tableName
}

func (tx *JSONListTx) Keys() []string {
	return []string{
		"ind",
	}
}

func (tx *JSONListTx) Client() Client {
	return tx.tx
}

func (tx *JSONListTx) Save(entry *ListEntry) error {
	return tx.Client().SQLExec("insert into $table$ values (?, ?);", entry.Index, entry.Value)
}

func (tx *JSONListTx) Update(entry *ListEntry) error {
	return tx.Client().SQLExec("update $table$ set value=? where ind=?;", entry.Index, entry.Value)
}

func (tx *JSONListTx) Upsert(entry *ListEntry) error {
	err := tx.Save(entry)
	if err != nil && IsPrimaryKeyConstraintError(err) {
		return tx.Update(entry)
	}
	return err
}

func (tx *JSONListTx) Append(entry *ListEntry) error {
	return tx.Client().SQLExec("insert into $table$ (value) values (?);", entry.Value)
}

func (tx *JSONListTx) GetAt(index int64) (*ListEntry, error) {
	o, err := tx.Client().SQLQueryFirst("select * from $table$ where ind=?;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (tx *JSONListTx) MinIndex() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select min(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *JSONListTx) MaxIndex() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select max(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *JSONListTx) Count() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select count(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *JSONListTx) Size(index int64) (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(length(value), 0) from $table$ where ind=?;", IntScanner, index)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *JSONListTx) TotalSize() (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *JSONListTx) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := tx.Client().SQLQueryFirst("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (tx *JSONListTx) RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error) {
	c, err := tx.Client().SQLQuery("select * from $table$ where ind>? order by ind limit ?, ?;", ListEntryScanner, index, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()
	var entries []*ListEntry
	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*ListEntry))
	}
	return entries, nil
}

func (tx *JSONListTx) Range(offset, count int) ([]*ListEntry, error) {
	c, err := tx.Client().SQLQuery("select * from $table$ order by ind limit ?, ?;", ListEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*ListEntry
	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*ListEntry))
	}
	return entries, nil
}

func (tx *JSONListTx) AllBefore(index int64) (Cursor, error) {
	return tx.Client().SQLQuery("select * from $table$ where ind<=? order by ind;", ListEntryScanner, index)
}

func (tx *JSONListTx) AllAfter(index int64) (Cursor, error) {
	return tx.Client().SQLQuery("select * from $table$ where ind>=? order by ind;", ListEntryScanner, index)
}

func (tx *JSONListTx) Delete(index int64) error {
	return tx.Client().SQLExec("delete from $table$ where ind=?;", index)
}

func (tx *JSONListTx) Clear() error {
	return tx.Client().SQLExec("delete from $table$;")
}

func (tx *JSONListTx) EditAt(index int64, path string, sqlValue string) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where ind=?;", path, sqlValue)
	return tx.Client().SQLExec(rawQuery, index)
}

func (tx *JSONListTx) ExtractAt(index int64, path string) (string, error) {
	rawQuery := fmt.Sprintf(
		"select json_unquote(json_extract(value, '%s')) from $table$ where ind=?;", path)
	o, err := tx.Client().SQLQueryFirst(rawQuery, StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *JSONListTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s);",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONListTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where %s",
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return tx.Client().SQLExec(rawQuery)
}

func (tx *JSONListTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(value, '%s')) from $table$ where %s;",
		path,
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONListTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName)
}

func (tx *JSONListTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	return tx.Client().SQLQuery(rawQuery, scannerName, offset, count)
}

func (tx *JSONListTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *JSONListTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *JSONListTx) TX() *TX {
	return tx.tx
}
