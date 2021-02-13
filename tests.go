package bome

import (
	"fmt"
	"os"
)

var (
	doc1 = `{
	"name": "zebou", 
	"age": 23,
	"profession": "Commerçante",
 	"address": {
		"city": "Abidjan",
		"region": "Region des lagunes",
		"commune": "Bingerville",
		"quartier": "Cités EMPT"
	}
	}`
	doc2 = `{
	"name": "wassiath", 
	"age": 29,
	"profession": "Commerciale Canal",
 	"address": {
		"city": "Abidjan",
		"region": "Region des lagunes",
		"commune": "Yopougon",
		"quartier": "Le sable"
	}
	}`
	doc3 = `{
	"name": "akam", 
	"age": 35,
	"profession": "Assistante sociale",
 	"address": {
		"city": "Bonoua",
		"region": "Region des lagunes"
		}
	}`

	testDBPath  string
	testDialect string
)

func init() {
	testDBPath = os.Getenv("BOME_TESTS_DB")
	fmt.Println("ENV.DBPATH = ", testDBPath)
	if testDBPath == "" {
		testDBPath = ":memory:"
	}

	testDialect = os.Getenv("BOME_TESTS_DIALECT")
	fmt.Println("ENV.DIALECT = ", testDialect)
	if testDialect == "" {
		testDialect = SQLite3
	}
}
