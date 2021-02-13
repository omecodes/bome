package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
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

func initDbMap(t *testing.T) {
	if dbMap == nil {
		var err error
		db, err = sql.Open(testDialect, testDBPath)
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

func TestDict_Save(t *testing.T) {
	Convey("Adding entry to map", t, func() {
		initDbMap(t)
		err := dbMap.Save(&mapEntry1)
		So(err, ShouldBeNil)

		err = dbMap.Save(&mapEntry1)
		So(err, ShouldNotBeNil)

		err = dbMap.Save(&mapEntry2)
		So(err, ShouldBeNil)

		err = dbMap.Save(&mapEntry3)
		So(err, ShouldBeNil)

		err = dbMap.Save(&mapEntry4)
		So(err, ShouldBeNil)
	})
}

func TestDict_Contains(t *testing.T) {
	Convey("Test Map contains key k1", t, func() {
		initDbMap(t)
		contains, err := dbMap.Contains("k1")
		So(err, ShouldBeNil)
		So(contains, ShouldBeTrue)
	})
}

func TestDict_Range(t *testing.T) {
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

func TestDict_Get(t *testing.T) {
	Convey("Test Map contains key k1", t, func() {
		initDbMap(t)
		value, err := dbMap.Get("k1")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "v1")
	})
}

func TestDict_Delete(t *testing.T) {
	Convey("Test Map item removal", t, func() {
		initDbMap(t)
		err := dbMap.Delete("k1")
		So(err, ShouldBeNil)

		_, err = dbMap.Get("k1")
		So(IsNotFound(err), ShouldBeTrue)
	})
}

func TestDict_List(t *testing.T) {
	Convey("List map entries", t, func() {
		initDbMap(t)

		cursor, err := dbMap.List()
		So(err, ShouldBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			So(o, ShouldNotBeNil)

			entry := o.(*MapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestDict_Clear(t *testing.T) {
	Convey("Clear map entries", t, func() {
		initDbMap(t)

		err := dbMap.Clear()
		So(err, ShouldBeNil)

		cursor, err := dbMap.List()
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			So(o, ShouldNotBeNil)

			entry := o.(*MapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 0)
	})
}

func TestDict_Close(t *testing.T) {
	Convey("Close Map", t, func() {
		initDbMap(t)
		err := dbMap.Close()
		So(err, ShouldBeNil)
	})
}
