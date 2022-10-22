package bome

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/omecodes/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	dbMap *Map

	mapEntry1 = MapEntry{Key: "k1", Value: "v1"}
	mapEntry2 = MapEntry{Key: "k2", Value: "v2"}
	mapEntry3 = MapEntry{Key: "k3", Value: "v3"}
	mapEntry4 = MapEntry{Key: "k4", Value: "v4"}
)

func init() {
	_ = os.Remove(testDBPath)
}

func initDbMap(_ *testing.T) {
	if dbMap == nil {
		var err error
		db, err := sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists map")
		So(err, ShouldBeNil)

		dbMap, err = Build().SetConn(db).SetDialect("unsupported").SetTableName("map").Map()
		So(err, ShouldNotBeNil)
		So(dbMap, ShouldBeNil)

		dbMap, err = Build().SetConn(db).SetDialect(testDialect).SetTableName("map").Map()
		So(err, ShouldBeNil)
		So(dbMap, ShouldNotBeNil)
	}
}

func TestNewSQLMap(t *testing.T) {
	Convey("SQL Open", t, func() {
		initDbMap(t)
	})
}

func TestMap_Save(t *testing.T) {
	Convey("Adding entry to map", t, func() {
		initDbMap(t)
		err := dbMap.SaveRaw(mapEntry1.Key, mapEntry1.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.Save(mapEntry1.Key, mapEntry1.Value, SaveOptions{})
		So(err, ShouldNotBeNil)

		err = dbMap.SaveRaw(mapEntry2.Key, mapEntry2.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.SaveRaw(mapEntry3.Key, mapEntry3.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.SaveRaw(mapEntry4.Key, mapEntry4.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.SaveRaw("zebou", doc1, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.SaveRaw("wassiath", doc2, SaveOptions{})
		So(err, ShouldBeNil)

		err = dbMap.SaveRaw("akam", doc3, SaveOptions{})
		So(err, ShouldBeNil)
	})
}

func TestMap_Contains(t *testing.T) {
	Convey("Test Map contains key k1", t, func() {
		initDbMap(t)
		exists, err := dbMap.Contains("k1")
		So(err, ShouldBeNil)
		So(exists, ShouldBeTrue)
	})
}

func TestMap_Range(t *testing.T) {
	Convey("Range", t, func() {
		initDbMap(t)
		entries, err := dbMap.Range(1, 3)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 3)

		for _, entry := range entries {
			So(entry.Value, ShouldBeIn, "v2", "v3", "v4")
		}
	})
}

func TestMap_Get(t *testing.T) {
	Convey("Test Map contains key k1", t, func() {
		initDbMap(t)
		value, err := dbMap.GetRaw("k1")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "v1")
	})
}

func TestMap_Delete(t *testing.T) {
	Convey("Test Map item removal", t, func() {
		initDbMap(t)
		err := dbMap.Delete("k1")
		So(err, ShouldBeNil)

		_, err = dbMap.GetRaw("k1")
		So(errors.IsNotFound(err), ShouldBeTrue)
	})
}

func TestMap_List(t *testing.T) {
	Convey("List map entries", t, func() {
		initDbMap(t)

		c, err := dbMap.List()
		So(err, ShouldBeNil)

		var entries []*MapEntry
		for c.HasNext() {
			o, err := c.Entry()
			So(err, ShouldBeNil)
			So(o, ShouldNotBeNil)

			entry := o.(*MapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 6)
	})
}

func TestMap_EditAt(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		err := dbMap.EditAt("akam", "$.address.commune", StringExpr("yahou"))
		So(err, ShouldBeNil)

		value, err := dbMap.ExtractAt("akam", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "yahou")
	})
}

func TestMap_ExtractAt(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		value, err := dbMap.ExtractAt("wassiath", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "Yopougon")
	})
}

func TestJsonMap_Clear(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		err := dbMap.Clear()
		So(err, ShouldBeNil)
	})
}

func TestMap_Clear(t *testing.T) {
	Convey("Clear map entries", t, func() {
		initDbMap(t)

		err := dbMap.Clear()
		So(err, ShouldBeNil)

		cursor, err := dbMap.List()
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Entry()
			So(err, ShouldBeNil)
			So(o, ShouldNotBeNil)

			entry := o.(*MapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 0)
	})
}

func TestMap_Close(t *testing.T) {
	Convey("Close Map", t, func() {
		initDbMap(t)
		err := dbMap.Close()
		So(err, ShouldBeNil)
	})
}
