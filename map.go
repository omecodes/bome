package bome

import "database/sql"

// Map is a convenience for persistent string to string dict
type Map interface {
	Save(entry *MapEntry) error
	Get(key string) (string, error)
	Contains(key string) (bool, error)
	Delete(key string) error
	List() (Cursor, error)
	Clear() error
	Close() error
}

type dict struct {
	*Bome
}

func (d *dict) Save(entry *MapEntry) error {
	err := d.Exec("insert", entry.Key, entry.Value).Error
	if err != nil {
		err = d.Exec("update", entry.Value, entry.Key).Error
	}
	return err
}

func (d *dict) Get(key string) (string, error) {
	o, err := d.QueryFirst("select", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (d *dict) Contains(key string) (bool, error) {
	res, err := d.QueryFirst("contains", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (d *dict) Delete(key string) error {
	return d.Exec("delete", key).Error
}

func (d *dict) List() (Cursor, error) {
	return d.Query("select_all", MapEntrySCanner)
}

func (d *dict) Clear() error {
	return d.Exec("clear").Error
}

func (d *dict) Close() error {
	return d.Bome.sqlDb.Close()
}

// NewSQLMap creates an sql map which entries are store in a table that have name created with concataning name and '_map'
func NewSQLMap(dsn string, name string) (Map, error) {
	d := new(dict)
	db, err := Open(dsn)
	if err != nil {
		return nil, nil
	}
	d.Bome = db

	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$_map (name varchar(255) not null primary key, val longblob not null);").
		AddStatement("insert", "insert into $prefix$_mapping values (?, ?);").
		AddStatement("update", "update $prefix$_mapping set val=? where name=?;").
		AddStatement("select", "select value from $prefix$_mapping where name=?;").
		AddStatement("select_all", "select * from $prefix$_mapping;").
		AddStatement("contains", "select 1 from $prefix$_mapping where name=?;").
		AddStatement("delete", "delete from $prefix$_mapping where name=?;").
		AddStatement("clear", "delete from $prefix$_mapping;")
	return d, d.Init()
}

// MapFromSQLDB creates MySQL wrapped map
func MapFromSQLDB(dialect string, db *sql.DB, name string) (Map, error) {
	d := new(dict)
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
		AddTableDefinition("create table if not exists $prefix$_map (name varchar(255) not null primary key, value longtext not null);").
		AddStatement("insert", "insert into $prefix$_map values (?, ?);").
		AddStatement("update", "update $prefix$_map set value=? where name=?;").
		AddStatement("select", "select value from $prefix$_map where name=?;").
		AddStatement("select_all", "select * from $prefix$_map;").
		AddStatement("contains", "select 1 from $prefix$_map where name=?;").
		AddStatement("delete", "delete from $prefix$_map where name=?;").
		AddStatement("clear", "delete from $prefix$_map;")
	return d, d.Init()
}
