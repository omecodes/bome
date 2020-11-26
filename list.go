package bome

import (
	"database/sql"
	"log"
)

type List struct {
	*Bome
}

func (l *List) BeginTransaction() (*ListTx, error) {
	tx, err := l.BeginTx()
	if err != nil {
		return nil, err
	}
	return &ListTx{
		tx: tx,
	}, nil
}

func (l *List) ContinueTransaction(tx *TX) *ListTx {
	return &ListTx{
		tx: tx,
	}
}

func (l *List) Client() Client {
	return l.Bome
}

func (l *List) Save(entry *ListEntry) error {
	return l.Client().SQLExec("insert into $table$ values (?, ?);", entry.Index, entry.Value)
}

func (l *List) Append(entry *ListEntry) error {
	return l.Client().SQLExec("insert into $table$ (value) values (?);", entry.Value)
}

func (l *List) GetAt(index int64) (*ListEntry, error) {
	o, err := l.Client().SQLQueryFirst("select * from $table$ where ind=?;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *List) MinIndex() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select min(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) MaxIndex() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select max(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) Count() (int64, error) {
	res, err := l.Client().SQLQueryFirst("select count(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := l.Client().SQLQueryFirst("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *List) RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().SQLQuery("select * from $table$ where ind>? order by ind limit ?, ?;", ListEntryScanner, index, offset, count)
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

func (l *List) Range(offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().SQLQuery("select * from $table$ order by ind limit ?, ?;", ListEntryScanner, offset, count)
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

func (l *List) GetAllFromSeq(index int64) (Cursor, error) {
	return l.Client().SQLQuery("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
}

func (l *List) Delete(index int64) error {
	return l.Client().SQLExec("delete from $table$ where ind=?;", index)
}

func (l *List) Clear() error {
	return l.Client().SQLExec("delete from $table$;")
}

func (l *List) Close() error {
	return l.Bome.sqlDb.Close()
}

// NewList creates MySQL wrapped list
func NewList(db *sql.DB, dialect string, tableName string) (*List, error) {
	d := new(List)
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

	d.SetTableName(escaped(tableName)).
		AddTableDefinition(
			"create table if not exists $table$ (ind integer not null primary key $auto_increment$, value longtext not null);")
	err = d.init()
	return d, err
}
