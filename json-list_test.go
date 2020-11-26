package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	dbJsonList *JSONList
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

		_, err = db.Exec("drop table if exists j_list")
		So(err, ShouldBeNil)

		dbJsonList, err = NewJSONList(db, "unsupported", "j_list")
		So(err, ShouldNotBeNil)
		So(dbJsonList, ShouldBeNil)

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
	if jsonTestEnabled {
		Convey("Save double map entries", t, func() {
			initJsonList(t)

			var err error
			err = dbJsonList.Save(&ListEntry{Value: doc1})
			So(err, ShouldBeNil)

			err = dbJsonList.Save(&ListEntry{Value: doc2})
			So(err, ShouldBeNil)

			err = dbJsonList.Save(&ListEntry{Value: doc3})
			So(err, ShouldBeNil)
		})
	}
}

func TestJsonListDB_EditAt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit at", t, func() {
			initJsonList(t)
			err := dbJsonList.EditAt(3, "$.address.commune", "'Yahou'")
			So(err, ShouldBeNil)
		})
	}
}

func TestJsonListDB_ExtractAt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Extract at", t, func() {
			value, err := dbJsonList.ExtractAt(3, "$.address.commune")
			So(err, ShouldBeNil)
			So(value, ShouldEqual, "Yahou")
		})
	}
}

func TestJsonListDB_Clear(t *testing.T) {
	Convey("Clear all entries", t, func() {
		err := dbJsonList.Clear()
		So(err, ShouldBeNil)
	})
}
