package bome

import (
	"database/sql"
	"log"
)

type Map struct {
	tableName string
	*Bome
}

func (d *Map) BeginTransaction() (*MapTx, error) {
	tx, err := d.BeginTx()
	if err != nil {
		return nil, err
	}

	return &MapTx{
		tx: tx.clone(d.Bome),
	}, nil
}

func (d *Map) ContinueTransaction(tx *TX) *MapTx {
	return &MapTx{
		tx: tx.clone(d.Bome),
	}
}

func (d *Map) Client() Client {
	return d.Bome
}

func (d *Map) Save(entry *MapEntry) error {
	err := d.Client().SQLExec("insert into $table$ values (?, ?);", entry.Key, entry.Value)
	if err != nil {
		err = d.Client().SQLExec("update $table$ set value=? where name=?;", entry.Value, entry.Key)
	}
	return err
}

func (d *Map) Get(key string) (string, error) {
	o, err := d.Client().SQLQueryFirst("select value from $table$ where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (d *Map) Size(key string) (int64, error) {
	o, err := d.Client().SQLQueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (d *Map) TotalSize() (int64, error) {
	o, err := d.Client().SQLQueryFirst("select coalesce(sum(length(value), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (d *Map) Contains(key string) (bool, error) {
	res, err := d.Client().SQLQueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (d *Map) Range(offset, count int) ([]*MapEntry, error) {
	c, err := d.Client().SQLQuery("select * from $table$ limit ?, ?;", MapEntryScanner, offset, count)
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

func (d *Map) Delete(key string) error {
	return d.Client().SQLExec("delete from $table$ where name=?;", key)
}

func (d *Map) List() (Cursor, error) {
	return d.Client().SQLQuery("select * from $table$;", MapEntryScanner)
}

func (d *Map) Clear() error {
	return d.Client().SQLExec("delete from $table$;")
}

func (d *Map) Close() error {
	return d.Bome.sqlDb.Close()
}

// NewMap creates MySQL wrapped map
func NewMap(db *sql.DB, dialect string, tableName string) (*Map, error) {
	d := new(Map)
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
			"create table if not exists $table$ (name varchar(255) not null primary key, value longtext not null);")
	return d, d.Init()
}
