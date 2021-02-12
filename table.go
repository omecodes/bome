package bome

import "database/sql"

type Table struct {
	Schema  string
	DB      *sql.DB
	Dialect string
	Name    string
}
