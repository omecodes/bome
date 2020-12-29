package bome

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

const (
	mysqlIndexScanner  = "mysql_index_scanner"
	sqliteIndexScanner = "sqlite_index_scanner"

	// VarPrefix is used to set table name prefix dynamically
	VarPrefix = "$prefix$"

	VarTable = "$table$"

	// VarEngine is used to define prefix. Bome replaces it with the dialect engine value
	VarEngine = "$engine$"

	// VarAutoIncrement is used set auto_increment to int field. Bome replaces it with the dialect proper value
	VarAutoIncrement = "$auto_increment$"

	// VarLocate is the equivalent of string replace
	VarLocate = "$locate$"
)

// Result is returned when executing a write operation
type Result struct {
	Error        error
	LastInserted int64
	AffectedRows int64
}

// Bome is an SQL database driver
type Bome struct {
	sqlDb                      *sql.DB
	mux                        *sync.RWMutex
	dialect                    string
	isSQLite                   bool
	compiledStatements         map[string]*sql.Stmt
	vars                       map[string]string
	tableDefs                  []string
	registeredStatements       map[string]string
	registeredSQLiteStatements map[string]string
	registeredMySQLStatements  map[string]string
	migrationScripts           []string
	scanners                   map[string]Scanner
	initDone                   bool
}

// Open detects and creates an instance of Bome DB according to the dialect
func Open(dsn string) (*Bome, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "sqlite3" {
		db, err := sql.Open("sqlite3", strings.TrimPrefix(dsn, "sqlite3://"))
		if err != nil {
			return nil, err
		}

		dbome := new(Bome)
		dbome.sqlDb = db
		dbome.isSQLite = true
		dbome.dialect = "sqlite3"
		dbome.SetVariable(VarLocate, "instr")
		dbome.SetVariable(VarAutoIncrement, "AUTOINCREMENT")
		dbome.SetVariable(VarEngine, "")
		if _, err := dbome.sqlDb.Exec("PRAGMA foreign_keys=ON"); err != nil {
			return nil, err
		}
		dbome.mux = new(sync.RWMutex)
		return dbome, nil

	} else if u.Scheme == "mysql" {
		db, err := sql.Open("mysql", strings.TrimPrefix(dsn, "mysql://"))
		if err != nil {
			return nil, err
		}

		dbome := new(Bome)
		dbome.sqlDb = db
		dbome.dialect = "mysql"
		dbome.SetVariable(VarLocate, "locate")
		dbome.SetVariable(VarAutoIncrement, "AUTO_INCREMENT")
		dbome.SetVariable(VarEngine, "engine=InnoDB")
		return dbome, nil

	} else {
		return nil, DialectNotSupported
	}
}

// New creates a MySQL wrapper
func New(db *sql.DB) (*Bome, error) {
	dbome := new(Bome)
	dbome.sqlDb = db
	dbome.dialect = "mysql"
	dbome.SetVariable(VarLocate, "locate")
	dbome.SetVariable(VarAutoIncrement, "AUTO_INCREMENT")
	dbome.SetVariable(VarEngine, "engine=InnoDB")
	return dbome, nil
}

// NewLite creates an SQLite wrapper
func NewLite(db *sql.DB) (*Bome, error) {
	dbome := new(Bome)
	dbome.sqlDb = db
	dbome.isSQLite = true
	dbome.dialect = "sqlite3"
	dbome.SetVariable(VarLocate, "instr")
	dbome.SetVariable(VarAutoIncrement, "AUTOINCREMENT")
	dbome.SetVariable(VarEngine, "")
	if _, err := dbome.sqlDb.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return nil, err
	}
	dbome.mux = new(sync.RWMutex)
	return dbome, nil
}

// Init must be call after custom variable and statements are set. And before any request is executed
func (bome *Bome) Init() error {
	return bome.init()
}

func (bome *Bome) init() error {
	for name, scanner := range defaultScanners {
		bome.RegisterScanner(name, scanner)
	}
	bome.RegisterScanner(mysqlIndexScanner, NewScannerFunc(bome.mysqlIndexScan))
	bome.RegisterScanner(sqliteIndexScanner, NewScannerFunc(bome.sqliteIndexScan))

	if bome.tableDefs != nil && len(bome.tableDefs) > 0 {
		for _, schema := range bome.tableDefs {
			for name, value := range bome.vars {
				schema = strings.Replace(schema, name, value, -1)
			}
			_, err := bome.sqlDb.Exec(schema)
			if err != nil {
				return err
			}
		}
	}

	var specificStatements map[string]string
	if bome.isSQLite && bome.registeredSQLiteStatements != nil {
		specificStatements = bome.registeredSQLiteStatements
	} else {
		specificStatements = bome.registeredMySQLStatements
	}

	if specificStatements != nil {
		if bome.registeredStatements == nil {
			bome.registeredStatements = map[string]string{}
		}
		for name, stmt := range specificStatements {
			bome.registeredStatements[name] = stmt
		}
	}

	for name, stmt := range bome.registeredStatements {
		for name, value := range bome.vars {
			stmt = strings.Replace(stmt, name, value, -1)
		}
		bome.registeredStatements[name] = stmt
	}

	if bome.registeredStatements != nil && len(bome.registeredStatements) > 0 {
		bome.compiledStatements = map[string]*sql.Stmt{}
		for name, stmt := range bome.registeredStatements {
			/*for name, value := range bome.vars {
				stmt = strings.Replace(stmt, name, value, -1)
			} */
			compiledStmt, err := bome.sqlDb.Prepare(stmt)
			if err != nil {
				return err
			}
			bome.compiledStatements[name] = compiledStmt
		}
	}

	bome.initDone = true
	return nil
}

// Migrate executes registered migration scripts. And must be call before init
func (bome *Bome) Migrate() error {
	if !bome.initDone {
		return InitError
	}
	for _, ms := range bome.migrationScripts {
		for name, value := range bome.vars {
			ms = strings.Replace(ms, name, value, -1)
		}

		_, err := bome.sqlDb.Exec(ms)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsSQLite return true if wrapped database is SQLite
func (bome *Bome) IsSQLite() bool {
	return bome.isSQLite
}

// SetVariable is used to defines a variable
func (bome *Bome) SetVariable(name string, value string) *Bome {
	if bome.vars == nil {
		bome.vars = map[string]string{}
	}
	bome.vars[name] = value
	return bome
}

// SetTablePrefix is used to defines all table name prefix
func (bome *Bome) SetTablePrefix(prefix string) *Bome {
	if bome.vars == nil {
		bome.vars = map[string]string{}
	}
	bome.vars[VarPrefix] = prefix
	return bome
}

// SetTableName registers variable $table$ value
func (bome *Bome) SetTableName(tableName string) *Bome {
	if bome.vars == nil {
		bome.vars = map[string]string{}
	}
	bome.vars[VarTable] = tableName
	return bome
}

//AddMigrationScript adds an migration script.
func (bome *Bome) AddMigrationScript(s string) *Bome {
	bome.migrationScripts = append(bome.migrationScripts, s)
	return bome
}

// AddTableDefinition adds a table definition. Query can contains predefined or custom defined variables
func (bome *Bome) AddTableDefinition(schema string) *Bome {
	bome.tableDefs = append(bome.tableDefs, schema)
	return bome
}

// BeginTx begins a transaction
func (bome *Bome) BeginTx() (*TX, error) {
	tx, err := bome.sqlDb.Begin()
	if err != nil {
		return nil, err
	}

	tr := &TX{}
	tr.Tx = tx
	tr.bome = bome
	return tr, nil
}

// AddUniqueIndex adds a table index
func (bome *Bome) AddUniqueIndex(index Index, forceUpdate bool) error {
	if !bome.initDone {
		return InitError
	}

	for varName, value := range bome.vars {
		index.Table = strings.Replace(index.Table, varName, value, -1)
	}
	hasIndex, err := bome.TableHasIndex(index)
	if err != nil {
		return err
	}

	var result *Result
	if hasIndex && forceUpdate {
		var dropIndexSQL string
		if bome.dialect == "mysql" {
			dropIndexSQL = fmt.Sprintf("drop index %s on %s", index.Name, index.Table)
		} else {
			dropIndexSQL = fmt.Sprintf("drop index if exists %s on %s", index.Name, index.Table)
		}

		result = bome.RawExec(dropIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	if !hasIndex || forceUpdate {
		var createIndexSQL string
		if bome.dialect == "mysql" {
			createIndexSQL = fmt.Sprintf("create unique index %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		} else {
			createIndexSQL = fmt.Sprintf("create unique index if not exists %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		}

		result = bome.RawExec(createIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	return nil
}

// AddForeignKey
func (bome *Bome) AddForeignKey(fk *ForeignKey) error {
	o, err := bome.RawQueryFirst("SELECT 1 FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME=?", BoolScanner, fk.Name)
	if err != nil {
		if !IsNotFound(err) {
			return err
		}
	}

	if o != nil && !o.(bool) {
		r := bome.RawExec(fk.AlterTableAddQuery())
		return r.Error
	}
	return nil
}

// AddStatement registers a statement that can later be called with the given name
func (bome *Bome) AddStatement(name string, statementStr string) *Bome {
	if bome.registeredStatements == nil {
		bome.registeredStatements = map[string]string{}
	}
	bome.registeredStatements[name] = statementStr
	return bome
}

// AddSQLiteStatement registers an specific SQLite statement that can later be called with the given name
func (bome *Bome) AddSQLiteStatement(name string, statementStr string) *Bome {
	if bome.registeredSQLiteStatements == nil {
		bome.registeredSQLiteStatements = map[string]string{}
	}
	bome.registeredSQLiteStatements[name] = statementStr
	return bome
}

// AddMySQLStatement registers a specific MySQL statement that can later be called with the given name
func (bome *Bome) AddMySQLStatement(name string, statementStr string) *Bome {
	if bome.registeredMySQLStatements == nil {
		bome.registeredMySQLStatements = map[string]string{}
	}
	bome.registeredMySQLStatements[name] = statementStr
	return bome
}

// RegisterScanner registers a scanner with a name wich is used when querying data
func (bome *Bome) RegisterScanner(name string, scanner Scanner) *Bome {
	if bome.scanners == nil {
		bome.scanners = map[string]Scanner{}
	}
	bome.scanners[name] = scanner
	return bome
}

// TableHasIndex tells if the given index exists
func (bome *Bome) TableHasIndex(index Index) (bool, error) {
	if !bome.initDone {
		return false, InitError
	}

	var (
		scannerName string
		rawQuery    string
	)
	if bome.dialect == "mysql" {
		rawQuery = fmt.Sprintf("SHOW INDEX FROM %s", index.Table)
		scannerName = mysqlIndexScanner
	} else {
		rawQuery = fmt.Sprintf("PRAGMA INDEX_LIST('%s')", index.Table)
		scannerName = sqliteIndexScanner
	}

	cursor, err := bome.RawQuery(rawQuery, scannerName)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = cursor.Close()
	}()

	for cursor.HasNext() {
		ind, err := cursor.Next()
		if err != nil {
			return false, err
		}

		rowIndex := ind.(Index)
		if rowIndex.Name == index.Name {
			return true, nil
		}
	}
	return false, nil
}

// RawQuery executes a raw query.
// scannerName: is one the registered scanner name
func (bome *Bome) RawQuery(query string, scannerName string, params ...interface{}) (Cursor, error) {
	for name, value := range bome.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := bome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

// RawQueryFirst gets the first result of the query result.
// scannerName: is one the registered scanner name
func (bome *Bome) RawQueryFirst(query string, scannerName string, params ...interface{}) (interface{}, error) {
	for name, value := range bome.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := bome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

// RawExec executes the given raw query
func (bome *Bome) RawExec(rawQuery string, params ...interface{}) *Result {
	bome.wLock()
	defer bome.wUnlock()
	var r sql.Result
	result := &Result{}
	for name, value := range bome.vars {
		rawQuery = strings.Replace(rawQuery, name, value, -1)
	}
	r, result.Error = bome.sqlDb.Exec(rawQuery, params...)
	if result.Error == nil && bome.dialect != "sqlite3" {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

// SQLQuery executes a raw query.
// scannerName: is one the registered scanner name
func (bome *Bome) SQLQuery(query string, scannerName string, params ...interface{}) (Cursor, error) {
	for name, value := range bome.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := bome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

// SQLQueryFirst gets the first result of the query result.
// scannerName: is one the registered scanner name
func (bome *Bome) SQLQueryFirst(query string, scannerName string, params ...interface{}) (interface{}, error) {
	for name, value := range bome.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := bome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

// SQLExec executes the given raw query
func (bome *Bome) SQLExec(rawQuery string, params ...interface{}) error {
	bome.wLock()
	defer bome.wUnlock()
	for name, value := range bome.vars {
		rawQuery = strings.Replace(rawQuery, name, value, -1)
	}
	_, err := bome.sqlDb.Exec(rawQuery, params...)
	return err
}

// Query gets the results of the registered statement which name equals stmt
func (bome *Bome) Query(stmt string, scannerName string, params ...interface{}) (Cursor, error) {
	bome.rLock()
	defer bome.rUnLock()

	st := bome.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	return cursor, nil
}

// QueryFirst gets the first result of the registered statement which name equals stmt
func (bome *Bome) QueryFirst(stmt string, scannerName string, params ...interface{}) (interface{}, error) {
	bome.rLock()
	defer bome.rUnLock()

	st := bome.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := bome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

// Exec executes the registered statement which name match 'stmt'
func (bome *Bome) Exec(stmt string, params ...interface{}) *Result {
	bome.wLock()
	defer bome.wUnlock()

	result := &Result{}
	var (
		st *sql.Stmt
		r  sql.Result
	)

	st, result.Error = bome.findCompileStatement(stmt)
	if result.Error != nil {
		return result
	}

	r, result.Error = st.Exec(params...)
	if result.Error == nil {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

func (bome *Bome) sqliteIndexScan(row Row) (interface{}, error) {
	bome.rLock()
	defer bome.rUnLock()

	var index Index
	m, err := bome.rowToMap(row.(*sql.Rows))
	if err != nil {
		return nil, err
	}

	var ok bool
	index.Name, ok = m["name"].(string)
	if !ok {
		return nil, IndexNotFound
	}
	return index, nil
}

func (bome *Bome) mysqlIndexScan(row Row) (interface{}, error) {
	var index Index
	m, err := bome.rowToMap(row.(*sql.Rows))
	if err != nil {
		return nil, err
	}

	index.Name = fmt.Sprintf("%s", m["Key_name"])
	if index.Name == "" {
		return nil, IndexNotFound
	}
	index.Table = fmt.Sprintf("%s", m["Table"])
	if index.Table == "" {
		return nil, TableNotFound
	}
	return index, nil
}

func (bome *Bome) rowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	cols, _ := rows.Columns()
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}
	return m, nil

}

func (bome *Bome) findCompileStatement(name string) (*sql.Stmt, error) {
	if bome.compiledStatements == nil {
		return nil, StatementNotFound
	}

	if compiledStmt, found := bome.compiledStatements[name]; found {
		return compiledStmt, nil
	}
	return nil, StatementNotFound
}

func (bome *Bome) findScanner(name string) (Scanner, error) {
	scanner, found := bome.scanners[name]
	if !found {
		return nil, ScannerNotFound
	}
	return scanner, nil
}

func (bome *Bome) getStatement(name string) *sql.Stmt {
	if bome.compiledStatements == nil {
		return nil
	}
	s, found := bome.compiledStatements[name]
	if !found {
		return nil
	}
	return s
}

func (bome *Bome) rLock() {
	if bome.mux != nil {
		bome.mux.RLock()
	}
}

func (bome *Bome) wLock() {
	if bome.mux != nil {
		bome.mux.Lock()
	}
}

func (bome *Bome) rUnLock() {
	if bome.mux != nil {
		bome.mux.RUnlock()
	}
}

func (bome *Bome) wUnlock() {
	if bome.mux != nil {
		bome.mux.Unlock()
	}
}

type scannerFunc struct {
	f func(rows Row) (interface{}, error)
}

func (sf *scannerFunc) ScanRow(row Row) (interface{}, error) {
	return sf.f(row)
}

// NewScannerFunc creates a new scanner from function
func NewScannerFunc(f func(row Row) (interface{}, error)) Scanner {
	return &scannerFunc{
		f: f,
	}
}
