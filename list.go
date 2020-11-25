package bome

import (
	"database/sql"
	"log"
)

// List is a convenience for persistence list
type List interface {
	Save(*ListEntry) error
	Append(*ListEntry) error
	GetAt(index int64) (*ListEntry, error)
	GetNextFromSeq(index int64) (*ListEntry, error)
	GetAllFromSeq(index int64) (Cursor, error)
	RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error)
	Range(offset, count int) ([]*ListEntry, error)
	Delete(index int64) error
	MinIndex() (int64, error)
	MaxIndex() (int64, error)
	Count() (int64, error)
	Clear() error
	Close() error
}

type listDB struct {
	*Bome
}

func (l *listDB) BeginTransaction() (ListTransaction, error) {
	tx, err := l.BeginTx()
	if err != nil {
		return nil, err
	}
	return &txList{
		listDB: l,
		tx:     tx,
	}, nil
}

func (l *listDB) ContinueTransaction(tx *TX) ListTransaction {
	return &txList{
		listDB: l,
		tx:     tx,
	}
}

func (l *listDB) Client() Client {
	return l.Bome
}

func (l *listDB) Save(entry *ListEntry) error {
	return l.Client().SQLExec("insert into $prefix$ values (?, ?);", entry.Index, entry.Value)
}

func (l *listDB) Append(entry *ListEntry) error {
	return l.Client().SQLExec("insert into $prefix$ (value) values (?);", entry.Value)
}

func (l *listDB) GetAt(index int64) (*ListEntry, error) {
	o, err := l.Client().SQLQueryFirst("select * from $prefix$ where ind=?;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *listDB) MinIndex() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select min(ind) from $prefix$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) MaxIndex() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select max(ind) from $prefix$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) Count() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select count(ind) from $prefix$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := l.Client().SQLQueryFirst("select * from $prefix$ where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *listDB) RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().SQLQuery("select * from $prefix$ where ind>? order by ind limit ?, ?;", ListEntryScanner, index, offset, count)
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

func (l *listDB) Range(offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().SQLQuery("select * from $prefix$ order by ind limit ?, ?;", ListEntryScanner, offset, count)
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

func (l *listDB) GetAllFromSeq(index int64) (Cursor, error) {
	return l.Client().SQLQuery("select * from $prefix$ where ind>? order by ind;", ListEntryScanner, index)
}

func (l *listDB) Delete(index int64) error {
	return l.Client().SQLExec("delete from $prefix$ where ind=?;", index)
}

func (l *listDB) Clear() error {
	return l.Client().SQLExec("delete from $prefix$;")
}

func (l *listDB) Close() error {
	return l.Bome.sqlDb.Close()
}

// NewList creates MySQL wrapped list
func NewList(db *sql.DB, dialect string, tableName string) (List, error) {
	d := new(listDB)
	var err error

	if dialect == SQLite3 {
		d.Bome, err = NewLite(db)
	} else if dialect == MySQL {
		d.Bome, err = New(db)
	} else {
		return nil, DialectNotSupported
	}

	if err != nil {
		return nil, err
	}

	d.SetTablePrefix(tableName).
		AddTableDefinition(
			"create table if not exists $prefix$ (ind integer not null primary key $auto_increment$, value longtext not null);")
	err = d.init()
	return d, err
}
