package bome

// List is a convenience for persistence list
type List interface {
	Append(*ListEntry) error
	GetAt(index int64) (string, error)
	GetNextFromSeq(index int64) (string, error)
	GetAllFromSeq(index int64) (Cursor, error)
	Delete(index int64) error
	MinIndex() (int64, error)
	MaxIndex() (int64, error)
	Count() (int64, error)
	Clear() error
	Close() error
}

type listDB struct {
	*DB
}

func (l *listDB) Append(entry *ListEntry) error {
	return l.Exec("insert", entry.Value).Error
}

func (l *listDB) GetAt(index int64) (string, error) {
	o, err := l.QueryFirst("select", StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (l *listDB) MinIndex() (int64, error) {
	res, err := l.QueryFirst("select_min_index", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) MaxIndex() (int64, error) {
	res, err := l.QueryFirst("select_min_index", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) Count() (int64, error) {
	res, err := l.QueryFirst("select_count", "index")
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *listDB) GetNextFromSeq(index int64) (string, error) {
	o, err := l.QueryFirst("select_from", StringScanner, IntScanner)
	if err != nil {
		return "", err
	}
	return o.(string), nil
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
	return l.DB.sqlDb.Close()
}

// NewSQLList create an sql list by wrapping a sql database connexion
func NewSQLList(dsn, name string) (List, error) {
	d := new(listDB)
	var err error
	d.DB, err = Create(dsn)
	if err != nil {
		return nil, err
	}

	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$_list (ind int not null primary key $auto_increment$, encoded longblob not null);").
		AddStatement("insert", "insert into $prefix$_list (encoded) values (?);").
		AddStatement("select", "select * from $prefix$_list where ind=?;").
		AddStatement("select_min_index", "select min(ind) from $prefix$_list;").
		AddStatement("select_max_index", "select max(ind) from $prefix$_list;").
		AddStatement("select_count", "select count(ind) from $prefix$_list;").
		AddStatement("select_from", "select * from $prefix$_list where ind>? order by ind;").
		AddStatement("delete_by_seq", "delete from $prefix$_list where ind=?;").
		AddStatement("clear", "delete from $prefix$_list;")
	return d, d.Init()
}