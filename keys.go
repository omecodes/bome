package bome

import (
	"fmt"
	"strings"
)

type Keys struct {
	Table  string
	Fields []string
}

type ForeignKey struct {
	Name            string
	Table           *Keys
	References      *Keys
	OnDeleteCascade bool
}

func (fk *ForeignKey) AlterTableAddQuery() string {
	addForeignKeySQL := fmt.Sprintf(
		"alter table %s add constraint %s foreign key (%s) references %s(%s)",
		fk.Table.Table,
		fk.Name,
		strings.Join(fk.Table.Fields, ","),
		fk.References.Table,
		strings.Join(fk.References.Fields, ","),
	)
	if fk.OnDeleteCascade {
		addForeignKeySQL += " on delete cascade"
	}
	return addForeignKeySQL
}

func (fk *ForeignKey) InTableDefQuery() string {
	addForeignKeySQL := fmt.Sprintf(
		"foreign key (%s) references %s(%s)",
		strings.Join(fk.Table.Fields, ","),
		fk.References.Table,
		strings.Join(fk.References.Fields, ","),
	)
	if fk.OnDeleteCascade {
		addForeignKeySQL += " on delete cascade"
	}
	return addForeignKeySQL
}

// Index is the equivalent of SQL index.
type Index struct {
	Name   string
	Table  string
	Fields []string
}

func (ind *Index) MySQLDropQuery() string {
	return fmt.Sprintf("drop index %s on %s", ind.Name, ind.Table)
}

func (ind *Index) SQLiteDropQuery() string {
	return fmt.Sprintf("drop index if exists %s", ind.Name)
}

func (ind *Index) MySQLAddQuery() string {
	return fmt.Sprintf("create unique index %s on %s(%s)", ind.Name, ind.Table, strings.Join(ind.Fields, ","))
}

func (ind *Index) SQLiteAddQuery() string {
	return fmt.Sprintf("create unique index if not exists %s on %s(%s)", ind.Name, ind.Table, strings.Join(ind.Fields, ","))
}
