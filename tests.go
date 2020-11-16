package bome

import (
	"fmt"
	"os"
)

var (
	doc1 = `{
	"name": "zebou", 
	"age": "23",
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
	"age": "29",
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
	"age": "35",
	"profession": "Assistante sociale",
 	"address": {
		"city": "Bonoua",
		"region": "Region des lagunes"
	}
	}`

	testDBPath      string
	testDialect     string
	jsonTestEnabled bool
)

func init() {
	testDBPath = os.Getenv("BOME_TESTS_DB")
	if testDBPath == "" {
		testDBPath = "tests.db"
	}

	jsonTestEnabled = "1" == os.Getenv("BOME_JSON_TESTS_ENABLED")

	testDialect = os.Getenv("BOME_TESTS_DIALECT")
	if testDialect == "" {
		testDialect = SQLite3
	}

	fmt.Println()
	fmt.Println()
	fmt.Println("BOME_TESTS_DIALECT: ", testDialect)
	fmt.Println("BOME_TESTS_DB     : ", testDBPath)
	fmt.Println("JSON_TESTS_ENABLED: ", jsonTestEnabled)
	fmt.Println()
	fmt.Println()
}
