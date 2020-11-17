package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	dbJsonList JSONList
)

func init() {
	_ = os.Remove(testDBPath)
}

func initJsonList(t *testing.T) {
	if dbJsonList == nil {
		var err error

		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table j_list")
		So(err, ShouldBeNil)

		dbJsonList, err = NewJSONList(db, testDialect, "j_list")
		So(err, ShouldBeNil)
		So(dbJsonList, ShouldNotBeNil)
	}
}

func TestNewJSONList(t *testing.T) {
	Convey("Db init", t, func() {
		initJsonList(t)
	})
}

func TestJsonListDB_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initJsonList(t)

		var err error
		err = dbJsonList.Append(&ListEntry{Value: doc1})
		So(err, ShouldBeNil)

		err = dbJsonList.Append(&ListEntry{Value: doc2})
		So(err, ShouldBeNil)

		err = dbJsonList.Append(&ListEntry{Value: doc3})
		So(err, ShouldBeNil)
	})
}

func TestJsonListDB_Clear(t *testing.T) {
	Convey("Clear all entries", t, func() {
		err := dbJsonList.Clear()
		So(err, ShouldBeNil)
	})
}
