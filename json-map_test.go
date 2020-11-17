package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	dbJsonMap JSONMap

	couple1 = MapEntry{
		Key:   "zebou",
		Value: doc1,
	}
	couple2 = MapEntry{
		Key:   "wassiath",
		Value: doc2,
	}
	couple3 = MapEntry{
		Key:   "akam",
		Value: doc3,
	}
)

func init() {
	_ = os.Remove(testDBPath)
}

func initJsonDbMap(t *testing.T) {
	if dbJsonMap == nil {
		var err error

		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table j_map")
		So(err, ShouldBeNil)

		dbJsonMap, err = NewJSONMap(db, testDialect, "j_map")
		So(err, ShouldBeNil)
		So(dbJsonMap, ShouldNotBeNil)
	}
}

func TestNewJSONMapDB(t *testing.T) {
	Convey("DB init", t, func() {
		initJsonDbMap(t)
	})
}

func TestJsonMap_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initJsonDbMap(t)

		var err error
		err = dbJsonMap.Save(&couple1)
		So(err, ShouldBeNil)

		err = dbJsonMap.Save(&couple2)
		So(err, ShouldBeNil)

		err = dbJsonMap.Save(&couple3)
		So(err, ShouldBeNil)
	})
}

func TestJsonMap_EditAt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			err := dbJsonMap.EditAt("akam", "$.address.commune", StringExpr("yahou"))
			So(err, ShouldBeNil)

			value, err := dbJsonMap.ExtractAt("akam", "$.address.commune")
			So(err, ShouldBeNil)
			So(value, ShouldEqual, "yahou")
		})
	}
}

func TestJsonMap_ExtractAt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			value, err := dbJsonMap.ExtractAt("wassiath", "$.address.commune")
			So(err, ShouldBeNil)
			So(value, ShouldEqual, "Yopougon")
		})
	}
}

func TestJsonMap_Clear(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			err := dbJsonMap.Clear()
			So(err, ShouldBeNil)
		})
	}
}
