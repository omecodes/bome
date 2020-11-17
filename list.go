package bome

import "database/sql"

// List is a convenience for persistence list
type List interface {
	Append(*ListEntry) error
	GetAt(index int64) (*ListEntry, error)
	GetNextFromSeq(index int64) (*ListEntry, error)
	GetAllFromSeq(index int64) (Cursor, error)
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

func (l *listDB) Append(entry *ListEntry) error {
	return l.Exec("insert", entry.Value).Error
}

func (l *listDB) GetAt(index int64) (*ListEntry, error) {
	o, err := l.QueryFirst("select", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *listDB) MinIndex() (int64, error) {
	res, err := l.QueryFirst("select_min_index", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) MaxIndex() (int64, error) {
	res, err := l.QueryFirst("select_max_index", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) Count() (int64, error) {
	res, err := l.QueryFirst("select_count", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := l.QueryFirst("select_from", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *listDB) GetAllFromSeq(index int64) (Cursor, error) {
	return l.Query("select_from", ListEntryScanner, index)
}

func (l *listDB) Delete(index int64) error {
	return l.Exec("delete_by_seq", index).Error
}

func (l *listDB) Clear() error {
	return l.Exec("clear").Error
}

func (l *listDB) Close() error {
	return l.Bome.sqlDb.Close()
}

// ListFromSQLDB creates MySQL wrapped list
func ListFromSQLDB(dialect string, db *sql.DB, name string) (List, error) {
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

	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$ (ind integer not null primary key $auto_increment$, value longtext not null);").
		AddStatement("insert", "insert into $prefix$ (value) values (?);").
		AddStatement("select", "select * from $prefix$ where ind=?;").
		AddStatement("select_min_index", "select min(ind) from $prefix$;").
		AddStatement("select_max_index", "select max(ind) from $prefix$;").
		AddStatement("select_count", "select count(ind) from $prefix$;").
		AddStatement("select_from", "select * from $prefix$ where ind>? order by ind;").
		AddStatement("delete_by_seq", "delete from $prefix$ where ind=?;").
		AddStatement("clear", "delete from $prefix$;")
	err = d.init()
	return d, err
}
