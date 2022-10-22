package bome

import (
	"database/sql"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var dbJsonList *List

func init() {
	_ = os.Remove(testDBPath)
}

func initJsonList(_ *testing.T) {
	if dbJsonList == nil {
		var err error

		db, err := sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists j_list")
		So(err, ShouldBeNil)

		dbJsonList, err = Build().SetConn(db).SetDialect("unsupported").SetTableName("j_list").List()
		So(err, ShouldNotBeNil)
		So(dbJsonList, ShouldBeNil)

		dbJsonList, err = Build().SetConn(db).SetDialect(testDialect).SetTableName("j_list").List()
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
		err = dbJsonList.Save(doc1)
		So(err, ShouldBeNil)

		err = dbJsonList.Save(doc2)
		So(err, ShouldBeNil)

		err = dbJsonList.Save(doc3)
		So(err, ShouldBeNil)
	})
}

func TestJsonListDB_EditAt(t *testing.T) {
	Convey("EditAllAt at", t, func() {
		initJsonList(t)
		err := dbJsonList.EditAt(3, "$.address.commune", StringExpr("Yahou"))
		So(err, ShouldBeNil)
	})
}

func TestJsonListDB_Read(t *testing.T) {
	Convey("Read", t, func() {
		o := struct {
			Age  int    `json:"age"`
			Name string `json:"name"`
		}{}
		err := dbJsonList.Read(3, &o)
		So(err, ShouldBeNil)
		So(o.Name, ShouldEqual, "akam")
	})
}

func TestJsonListDB_ExtractAt(t *testing.T) {
	Convey("Extract at", t, func() {
		value, err := dbJsonList.ExtractAt(3, "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "Yahou")
	})
}

func TestJsonListDB_Clear(t *testing.T) {
	Convey("Clear all entries", t, func() {
		err := dbJsonList.Clear()
		So(err, ShouldBeNil)
	})
}
