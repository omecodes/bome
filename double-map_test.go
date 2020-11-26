package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	dbDoubleMap *DoubleMap

	doubleMapEntry1  = DoubleMapEntry{FirstKey: "fk1", SecondKey: "sk1", Value: "fk1sk1"}
	doubleMapEntry11 = DoubleMapEntry{FirstKey: "fk1", SecondKey: "sk2", Value: "fk1sk2"}
	doubleMapEntry12 = DoubleMapEntry{FirstKey: "fk1", SecondKey: "sk3", Value: "fk1sk3"}

	doubleMapEntry2  = DoubleMapEntry{FirstKey: "fk2", SecondKey: "sk1", Value: "fk2sk1"}
	doubleMapEntry21 = DoubleMapEntry{FirstKey: "fk2", SecondKey: "sk2", Value: "fk2sk2"}
	doubleMapEntry22 = DoubleMapEntry{FirstKey: "fk2", SecondKey: "sk3", Value: "fk2sk3"}

	doubleMapEntry3  = DoubleMapEntry{FirstKey: "fk3", SecondKey: "sk1", Value: "fk3sk1"}
	doubleMapEntry31 = DoubleMapEntry{FirstKey: "fk3", SecondKey: "sk2", Value: "fk3sk2"}
	doubleMapEntry32 = DoubleMapEntry{FirstKey: "fk3", SecondKey: "sk3", Value: "fk3sk3"}

	doubleMapEntry4  = DoubleMapEntry{FirstKey: "fk4", SecondKey: "sk1", Value: "fk4sk1"}
	doubleMapEntry41 = DoubleMapEntry{FirstKey: "fk4", SecondKey: "sk2", Value: "fk4sk2"}
	doubleMapEntry42 = DoubleMapEntry{FirstKey: "fk4", SecondKey: "sk3", Value: "fk4sk3"}
)

func init() {
	_ = os.Remove(testDBPath)
}

func initDoubleDbMap(t *testing.T) {
	if dbDoubleMap == nil {
		var err error
		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists d_map")
		So(err, ShouldBeNil)

		dbDoubleMap, err = NewDoubleMap(db, "unsupported", "d_map")
		So(err, ShouldNotBeNil)
		So(dbDoubleMap, ShouldBeNil)

		dbDoubleMap, err = NewDoubleMap(db, testDialect, "d_map")
		So(err, ShouldBeNil)
		So(dbDoubleMap, ShouldNotBeNil)
	}
}

func TestDoubleMap_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initDoubleDbMap(t)

		var err error
		err = dbDoubleMap.Save(&doubleMapEntry1)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry11)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry12)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry12)
		So(err, ShouldBeNil)

		err = dbDoubleMap.Save(&doubleMapEntry2)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry21)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry22)
		So(err, ShouldBeNil)

		err = dbDoubleMap.Save(&doubleMapEntry3)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry31)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry32)
		So(err, ShouldBeNil)

		err = dbDoubleMap.Save(&doubleMapEntry4)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry41)
		So(err, ShouldBeNil)
		err = dbDoubleMap.Save(&doubleMapEntry42)
		So(err, ShouldBeNil)
	})
}

func TestDoubleMap_Contains(t *testing.T) {
	Convey("Contains", t, func() {
		initDoubleDbMap(t)
		contains, err := dbDoubleMap.Contains("fk1", "sk1")
		So(err, ShouldBeNil)
		So(contains, ShouldBeTrue)
	})
}

func TestDoubleMap_Get(t *testing.T) {
	Convey("Get item", t, func() {
		initDoubleDbMap(t)
		value, err := dbDoubleMap.Get("fk1", "sk1")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "fk1sk1")
	})
}

func TestDoubleMap_RangeMatchingFirstKey(t *testing.T) {
	Convey("Range matching first key", t, func() {
		initDoubleDbMap(t)
		entries, err := dbDoubleMap.RangeMatchingFirstKey("fk1", 1, 2)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 2)
		for _, entry := range entries {
			So(entry.Value, ShouldBeIn, "fk1sk2", "fk1sk3")
		}
	})
}

func TestDoubleMap_RangeMatchingSecondKey(t *testing.T) {
	Convey("Range matching first key", t, func() {
		initDoubleDbMap(t)
		entries, err := dbDoubleMap.RangeMatchingSecondKey("sk1", 1, 2)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 2)
		for _, entry := range entries {
			So(entry.Value, ShouldBeIn, "fk2sk1", "fk3sk1")
		}
	})
}

func TestDoubleMap_Range(t *testing.T) {
	Convey("Range", t, func() {
		initDoubleDbMap(t)
		entries, err := dbDoubleMap.Range(0, 2)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 2)
		for _, entry := range entries {
			So(entry.Value, ShouldBeIn, "fk1sk1", "fk1sk2")
		}
	})
}

func TestDoubleMap_GetForFirst(t *testing.T) {
	Convey("Get all item matching first key", t, func() {
		initDoubleDbMap(t)
		cursor, err := dbDoubleMap.GetForFirst("fk1")
		So(err, ShouldBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*MapEntry)
			So(entry.Key, ShouldBeIn, "sk1", "sk2", "sk3")
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestDoubleMap_GetForSecond(t *testing.T) {
	Convey("Get all item matching second key", t, func() {
		initDoubleDbMap(t)
		cursor, err := dbDoubleMap.GetForSecond("sk1")
		So(err, ShouldBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*MapEntry)
			So(entry.Key, ShouldBeIn, "fk1", "fk2", "fk3", "fk4")
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 4)
	})
}

func TestDoubleMap_Delete(t *testing.T) {
	Convey("Delete item", t, func() {
		initDoubleDbMap(t)
		err := dbDoubleMap.Delete("fk1", "sk1")
		So(err, ShouldBeNil)

		value, err := dbDoubleMap.Get("fk1", "sk1")
		So(IsNotFound(err), ShouldBeTrue)
		So(value, ShouldEqual, "")
	})
}

func TestDoubleMap_DeleteAllMatchingSecondKeyKey(t *testing.T) {
	Convey("Delete for first key", t, func() {
		initDoubleDbMap(t)
		err := dbDoubleMap.DeleteAllMatchingSecondKey("sk2")
		So(err, ShouldBeNil)

		cursor, err := dbDoubleMap.GetForFirst("fk1")
		So(err, ShouldBeNil)
		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*MapEntry)
			So(entry.Key, ShouldNotEqual, "sk2")
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 1)

		entry, err := dbDoubleMap.Get("fk2", "sk2")
		So(IsNotFound(err), ShouldBeTrue)
		So(entry, ShouldEqual, "")

		cursor, err = dbDoubleMap.GetForSecond("sk2")
		So(err, ShouldBeNil)
		entries = []*MapEntry{}
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*MapEntry)
			So(entry.Key, ShouldNotEqual, "fk2")
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 0)
	})
}

func TestDoubleMap_DeleteAllMatchingFirstKey(t *testing.T) {
	Convey("Delete for first key", t, func() {
		initDoubleDbMap(t)
		err := dbDoubleMap.DeleteAllMatchingFirstKey("fk1")
		So(err, ShouldBeNil)

		cursor, err := dbDoubleMap.GetForFirst("fk1")
		So(err, ShouldBeNil)

		var entries []*MapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*MapEntry)
			So(entry.Key, ShouldNotEqual, "sk1")
			entries = append(entries, entry)
		}
		So(len(entries), ShouldEqual, 0)
	})
}

func TestDoubleMap_GetAll(t *testing.T) {
	Convey("Get all remaining items", t, func() {
		initDoubleDbMap(t)

		cursor, err := dbDoubleMap.GetAll()
		So(err, ShouldBeNil)

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 6)
	})
}

func TestDoubleMap_Clear(t *testing.T) {
	Convey("Delete for first key", t, func() {
		initDoubleDbMap(t)
		err := dbDoubleMap.Clear()
		So(err, ShouldBeNil)

		cursor, err := dbDoubleMap.GetAll()
		So(err, ShouldBeNil)

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 0)
	})
}

func TestDoubleMap_Close(t *testing.T) {
	Convey("Delete for first key", t, func() {
		initDoubleDbMap(t)
		err := dbDoubleMap.Close()
		So(err, ShouldBeNil)
	})
}
