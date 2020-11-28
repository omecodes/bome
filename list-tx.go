package bome

import "log"

type ListTx struct {
	tableName string
	tx        *TX
}

func (tx *ListTx) Client() Client {
	return tx.tx
}

func (tx *ListTx) Save(entry *ListEntry) error {
	return tx.Client().SQLExec("insert into "+tx.tableName+" values (?, ?);", entry.Index, entry.Value)
}

func (tx *ListTx) Append(entry *ListEntry) error {
	return tx.Client().SQLExec("insert into "+tx.tableName+" (value) values (?);", entry.Value)
}

func (tx *ListTx) GetAt(index int64) (*ListEntry, error) {
	o, err := tx.Client().SQLQueryFirst("select * from "+tx.tableName+" where ind=?;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (tx *ListTx) MinIndex() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select min(ind) from "+tx.tableName+";", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *ListTx) MaxIndex() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select max(ind) from "+tx.tableName+";", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *ListTx) Count() (int64, error) {
	res, err := tx.Client().SQLQueryFirst("select count(ind) from "+tx.tableName+";", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (tx *ListTx) Size(index int64) (int, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(length(value), 0) from "+tx.tableName+" where ind=?;", IntScanner, index)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *ListTx) TotalSize() (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(sum(length(value), 0) from "+tx.tableName+";", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *ListTx) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := tx.Client().SQLQueryFirst("select * from "+tx.tableName+" where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (tx *ListTx) RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error) {
	c, err := tx.Client().SQLQuery("select * from "+tx.tableName+" where ind>? order by ind limit ?, ?;", ListEntryScanner, index, offset, count)
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

func (tx *ListTx) Range(offset, count int) ([]*ListEntry, error) {
	c, err := tx.Client().SQLQuery("select * from "+tx.tableName+" order by ind limit ?, ?;", ListEntryScanner, offset, count)
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

func (tx *ListTx) AllBefore(index int64) (Cursor, error) {
	return tx.Client().SQLQuery("select * from "+tx.tableName+" where ind<=? order by ind;", ListEntryScanner, index)
}

func (tx *ListTx) AllAfter(index int64) (Cursor, error) {
	return tx.Client().SQLQuery("select * from "+tx.tableName+" where ind>=? order by ind;", ListEntryScanner, index)
}

func (tx *ListTx) Delete(index int64) error {
	return tx.Client().SQLExec("delete from "+tx.tableName+" where ind=?;", index)
}

func (tx *ListTx) Clear() error {
	return tx.Client().SQLExec("delete from " + tx.tableName + ";")
}

func (tx *ListTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *ListTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *ListTx) TX() *TX {
	return tx.tx
}
