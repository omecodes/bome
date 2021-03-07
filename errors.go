package bome

import (
	"github.com/go-sql-driver/mysql"
	"github.com/mattn/go-sqlite3"
)

func isPrimaryKeyConstraintError(err error) bool {
	if err == nil {
		return false
	}
	if me, ok := err.(*mysql.MySQLError); ok {
		return me.Number == 1062

	} else if se, ok := err.(sqlite3.Error); ok {
		return se.ExtendedCode == 2067 || se.ExtendedCode == 1555
	}
	return false
}

func isForeignKeyConstraintError(err error) bool {
	if err == nil {
		return false
	}

	if me, ok := err.(*mysql.MySQLError); ok {
		return me.Number == 1216

	} else if se, ok := err.(sqlite3.Error); ok {
		return se.ExtendedCode == sqlite3.ErrConstraintForeignKey
	}
	return false
}
