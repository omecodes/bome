package bome

import "log"

type MapTx struct {
	tableName string
	tx        *TX
}

func (tx *MapTx) Table() string {
	return tx.tableName
}

func (tx *MapTx) Keys() []string {
	return []string{
		"name",
	}
}

func (tx *MapTx) Client() Client {
	return tx.tx
}

func (tx *MapTx) Save(entry *MapEntry) error {
	err := tx.Client().SQLExec("insert into $table$ values (?, ?);", entry.Key, entry.Value)
	if err != nil {
		err = tx.Client().SQLExec("update $table$ set value=? where name=?;", entry.Value, entry.Key)
	}
	return err
}

func (tx *MapTx) Get(key string) (string, error) {
	o, err := tx.Client().SQLQueryFirst("select value from $table$ where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *MapTx) Contains(key string) (bool, error) {
	res, err := tx.Client().SQLQueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (tx *MapTx) Size(key string) (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *MapTx) TotalSize() (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(sum(length(value), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *MapTx) Range(offset, count int) ([]*MapEntry, error) {
	c, err := tx.Client().SQLQuery("select * from $table$ limit ?, ?;", MapEntryScanner, offset, count)
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

func (tx *MapTx) Delete(key string) error {
	return tx.Client().SQLExec("delete from $table$ where name=?;", key)
}

func (tx *MapTx) List() (Cursor, error) {
	return tx.Client().SQLQuery("select * from $table$;", MapEntryScanner)
}

func (tx *MapTx) Clear() error {
	return tx.Client().SQLExec("delete from $table$;")
}

func (tx *MapTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *MapTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *MapTx) TX() *TX {
	return tx.tx
}
