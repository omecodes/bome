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

	// VarEngine is used to define prefix. Bome replaces it with the dialect engine value
	VarEngine = "$engine$"

	// VarAutoIncrement is used set autorincrement to int field. Bome replaces it with the dialect proper value
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

// Index is the equivalent of SQL index
type Index struct {
	Name   string
	Table  string
	Fields []string
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
func (dbome *Bome) Init() error {
	return dbome.init()
}

func (dbome *Bome) init() error {
	dbome.RegisterScanner(mysqlIndexScanner, NewScannerFunc(dbome.mysqlIndexScan))
	dbome.RegisterScanner(sqliteIndexScanner, NewScannerFunc(dbome.sqliteIndexScan))

	if dbome.tableDefs != nil && len(dbome.tableDefs) > 0 {
		for _, schema := range dbome.tableDefs {
			for name, value := range dbome.vars {
				schema = strings.Replace(schema, name, value, -1)
			}
			_, err := dbome.sqlDb.Exec(schema)
			if err != nil {
				return err
			}
		}
	}

	var specificStatements map[string]string
	if dbome.isSQLite && dbome.registeredSQLiteStatements != nil {
		specificStatements = dbome.registeredSQLiteStatements
	} else {
		specificStatements = dbome.registeredMySQLStatements
	}

	if specificStatements != nil {
		if dbome.registeredStatements == nil {
			dbome.registeredStatements = map[string]string{}
		}
		for name, stmt := range specificStatements {
			dbome.registeredStatements[name] = stmt
		}
	}

	for name, stmt := range dbome.registeredStatements {
		for name, value := range dbome.vars {
			stmt = strings.Replace(stmt, name, value, -1)
		}
		dbome.registeredStatements[name] = stmt
	}

	if dbome.registeredStatements != nil && len(dbome.registeredStatements) > 0 {
		dbome.compiledStatements = map[string]*sql.Stmt{}
		for name, stmt := range dbome.registeredStatements {
			/*for name, value := range dbome.vars {
				stmt = strings.Replace(stmt, name, value, -1)
			} */
			compiledStmt, err := dbome.sqlDb.Prepare(stmt)
			if err != nil {
				return err
			}
			dbome.compiledStatements[name] = compiledStmt
		}
	}

	dbome.initDone = true
	return nil
}

// Migrate executes registered migration scripts. And must be call before init
func (dbome *Bome) Migrate() error {
	if !dbome.initDone {
		return InitError
	}
	for _, ms := range dbome.migrationScripts {
		for name, value := range dbome.vars {
			ms = strings.Replace(ms, name, value, -1)
		}

		_, err := dbome.sqlDb.Exec(ms)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsSQLite return true if wrapped database is SQLite
func (dbome *Bome) IsSQLite() bool {
	return dbome.isSQLite
}

// SetVariable is used to defines a variable
func (dbome *Bome) SetVariable(name string, value string) *Bome {
	if dbome.vars == nil {
		dbome.vars = map[string]string{}
	}
	dbome.vars[name] = value
	return dbome
}

// SetTablePrefix is used to defines all table name prefix
func (dbome *Bome) SetTablePrefix(prefix string) *Bome {
	if dbome.vars == nil {
		dbome.vars = map[string]string{}
	}
	dbome.vars[VarPrefix] = prefix
	return dbome
}

//AddMigrationScript adds an migration script.
func (dbome *Bome) AddMigrationScript(s string) *Bome {
	dbome.migrationScripts = append(dbome.migrationScripts, s)
	return dbome
}

// AddTableDefinition adds a table definition. Query can contains predefined or custom defined variables
func (dbome *Bome) AddTableDefinition(schema string) *Bome {
	dbome.tableDefs = append(dbome.tableDefs, schema)
	return dbome
}

// BeginTx begins a transaction
func (dbome *Bome) BeginTx() (*TX, error) {
	tx, err := dbome.sqlDb.Begin()
	if err != nil {
		return nil, err
	}

	tr := &TX{}
	tr.Tx = tx
	tr.dbome = dbome
	return tr, nil
}

// AddUniqueIndex adds a table index
func (dbome *Bome) AddUniqueIndex(index Index, forceUpdate bool) error {
	if !dbome.initDone {
		return InitError
	}

	for varName, value := range dbome.vars {
		index.Table = strings.Replace(index.Table, varName, value, -1)
	}
	hasIndex, err := dbome.TableHasIndex(index)
	if err != nil {
		return err
	}

	var result *Result
	if hasIndex && forceUpdate {
		var dropIndexSQL string
		if dbome.dialect == "mysql" {
			dropIndexSQL = fmt.Sprintf("drop index %s on %s", index.Name, index.Table)
		} else {
			dropIndexSQL = fmt.Sprintf("drop index if exists %s", index.Name)
		}

		result = dbome.RawExec(dropIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	if !hasIndex || forceUpdate {
		var createIndexSQL string
		if dbome.dialect == "mysql" {
			createIndexSQL = fmt.Sprintf("create unique index %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		} else {
			createIndexSQL = fmt.Sprintf("create unique index if not exists %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		}

		result = dbome.RawExec(createIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	return nil
}

// AddStatement registers a statement that can later be called with the given name
func (dbome *Bome) AddStatement(name string, statementStr string) *Bome {
	if dbome.registeredStatements == nil {
		dbome.registeredStatements = map[string]string{}
	}
	dbome.registeredStatements[name] = statementStr
	return dbome
}

// AddSQLiteStatement registers an specific SQLite statement that can later be called with the given name
func (dbome *Bome) AddSQLiteStatement(name string, statementStr string) *Bome {
	if dbome.registeredSQLiteStatements == nil {
		dbome.registeredSQLiteStatements = map[string]string{}
	}
	dbome.registeredSQLiteStatements[name] = statementStr
	return dbome
}

// AddMySQLStatement registers a specific MySQL statement that can later be called with the given name
func (dbome *Bome) AddMySQLStatement(name string, statementStr string) *Bome {
	if dbome.registeredMySQLStatements == nil {
		dbome.registeredMySQLStatements = map[string]string{}
	}
	dbome.registeredMySQLStatements[name] = statementStr
	return dbome
}

// RegisterScanner registers a scanner with a name wich is used when querying data
func (dbome *Bome) RegisterScanner(name string, scanner Scanner) *Bome {
	if dbome.scanners == nil {
		dbome.scanners = map[string]Scanner{}
	}
	dbome.scanners[name] = scanner
	return dbome
}

// TableHasIndex tells if the given index exists
func (dbome *Bome) TableHasIndex(index Index) (bool, error) {
	if !dbome.initDone {
		return false, InitError
	}

	var (
		scannerName string
		rawQuery    string
	)
	if dbome.dialect == "mysql" {
		rawQuery = fmt.Sprintf("SHOW INDEX FROM %s", index.Table)
		scannerName = mysqlIndexScanner
	} else {
		rawQuery = fmt.Sprintf("PRAGMA INDEX_LIST('%s')", index.Table)
		scannerName = sqliteIndexScanner
	}

	cursor, err := dbome.RawQuery(rawQuery, scannerName)
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
func (dbome *Bome) RawQuery(query string, scannerName string, params ...interface{}) (Cursor, error) {
	for name, value := range dbome.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := dbome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := dbome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return newCursor(rows, scanner), nil
}

// RawQueryFirst gets the first result of the query result.
// scannerName: is one the registered scanner name
func (dbome *Bome) RawQueryFirst(query string, scannerName string, params ...interface{}) (interface{}, error) {
	for name, value := range dbome.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := dbome.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := dbome.findScanner(scannerName)
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
func (dbome *Bome) RawExec(rawQuery string) *Result {
	dbome.wLock()
	defer dbome.wUnlock()
	var r sql.Result
	result := &Result{}
	for name, value := range dbome.vars {
		rawQuery = strings.Replace(rawQuery, name, value, -1)
	}
	r, result.Error = dbome.sqlDb.Exec(rawQuery)
	if result.Error == nil && dbome.dialect != "sqlite3" {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

// Query gets the results of the registered statement which name equals stmt
func (dbome *Bome) Query(stmt string, scannerName string, params ...interface{}) (Cursor, error) {
	dbome.rLock()
	defer dbome.rUnLock()

	st := dbome.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := dbome.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := newCursor(rows, scanner)
	return cursor, nil
}

// QueryFirst gets the first result of the registered statement which name equals stmt
func (dbome *Bome) QueryFirst(stmt string, scannerName string, params ...interface{}) (interface{}, error) {
	dbome.rLock()
	defer dbome.rUnLock()

	st := dbome.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := dbome.findScanner(scannerName)
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
func (dbome *Bome) Exec(stmt string, params ...interface{}) *Result {
	dbome.wLock()
	defer dbome.wUnlock()

	result := &Result{}
	var (
		st *sql.Stmt
		r  sql.Result
	)

	st, result.Error = dbome.findCompileStatement(stmt)
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

func (dbome *Bome) sqliteIndexScan(row Row) (interface{}, error) {
	dbome.rLock()
	defer dbome.rUnLock()

	var index Index
	m, err := dbome.rowToMap(row.(*sql.Rows))
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

func (dbome *Bome) mysqlIndexScan(row Row) (interface{}, error) {
	var index Index
	m, err := dbome.rowToMap(row.(*sql.Rows))
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

func (dbome *Bome) rowToMap(rows *sql.Rows) (map[string]interface{}, error) {
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

func (dbome *Bome) findCompileStatement(name string) (*sql.Stmt, error) {
	if dbome.compiledStatements == nil {
		return nil, StatementNotFound
	}

	if compiledStmt, found := dbome.compiledStatements[name]; found {
		return compiledStmt, nil
	}
	return nil, StatementNotFound
}

func (dbome *Bome) findScanner(name string) (Scanner, error) {
	scanner, found := dbome.scanners[name]
	if !found {
		return nil, ScannerNotFound
	}
	return scanner, nil
}

func (dbome *Bome) getStatement(name string) *sql.Stmt {
	if dbome.compiledStatements == nil {
		return nil
	}
	s, found := dbome.compiledStatements[name]
	if !found {
		return nil
	}
	return s
}

func (dbome *Bome) rLock() {
	if dbome.mux != nil {
		dbome.mux.RLock()
	}
}

func (dbome *Bome) wLock() {
	if dbome.mux != nil {
		dbome.mux.Lock()
	}
}

func (dbome *Bome) rUnLock() {
	if dbome.mux != nil {
		dbome.mux.RUnlock()
	}
}

func (dbome *Bome) wUnlock() {
	if dbome.mux != nil {
		dbome.mux.Unlock()
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
