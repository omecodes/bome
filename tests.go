package bome

import (
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
	if testDBPath == "" {
		testDBPath = ":memory:"
	}

	testDialect = os.Getenv("BOME_TESTS_DIALECT")
	if testDialect == "" {
		testDialect = SQLite3
	}
}
