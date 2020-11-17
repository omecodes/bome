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
	return d.Query("select_all", MapEntryScanner)
}

func (d *dict) Clear() error {
	return d.Exec("clear").Error
}

func (d *dict) Close() error {
	return d.Bome.sqlDb.Close()
}

// NewMap creates MySQL wrapped map
func NewMap(db *sql.DB, dialect string, tableName string) (Map, error) {
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

	d.SetTablePrefix(tableName).
		AddTableDefinition("create table if not exists $prefix$ (name varchar(255) not null primary key, value longtext not null);").
		AddStatement("insert", "insert into $prefix$ values (?, ?);").
		AddStatement("update", "update $prefix$ set value=? where name=?;").
		AddStatement("select", "select value from $prefix$ where name=?;").
		AddStatement("select_all", "select * from $prefix$;").
		AddStatement("contains", "select 1 from $prefix$ where name=?;").
		AddStatement("delete", "delete from $prefix$ where name=?;").
		AddStatement("clear", "delete from $prefix$;")
	return d, d.Init()
}
