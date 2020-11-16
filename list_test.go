package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	list      List
	db        *sql.DB
	listItem1 = "item 1"
	listItem2 = "item 2"
	listItem3 = "item 3"
	listItem4 = "item 4"
)

func init() {
	_ = os.Remove(testDBPath)
}

func initList(t *testing.T) {
	if list == nil {
		var err error
		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		list, err = ListFromSQLDB(testDialect, db, "list")
		So(err, ShouldBeNil)
		So(list, ShouldNotBeNil)
	}
}

func TestNewSQLList(t *testing.T) {
	Convey("SQL Open", t, func() {
		initList(t)
	})
}

func TestListDB_Append(t *testing.T) {
	Convey("Adding items to list", t, func() {
		initList(t)

		err := list.Append(&ListEntry{Value: listItem1})
		So(err, ShouldBeNil)

		err = list.Append(&ListEntry{Value: listItem2})
		So(err, ShouldBeNil)

		err = list.Append(&ListEntry{Value: listItem3})
		So(err, ShouldBeNil)

		err = list.Append(&ListEntry{Value: listItem4})
		So(err, ShouldBeNil)
	})
}

func TestListDB_Count(t *testing.T) {
	Convey("Count items", t, func() {
		initList(t)
		count, err := list.Count()
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 4)
	})
}

func TestListDB_GetAllFromSeq(t *testing.T) {
	Convey("Get All from index 2", t, func() {
		initList(t)

		cursor, err := list.GetAllFromSeq(2)
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)

		var entries []*ListEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*ListEntry)
			So(entry.Index, ShouldBeGreaterThan, 2)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 2)
	})
}

func TestListDB_GetAt(t *testing.T) {
	Convey("Get item at position 3", t, func() {
		initList(t)
		entry, err := list.GetAt(3)
		So(err, ShouldBeNil)
		So(entry, ShouldEqual, "item 3")
	})
}

func TestListDB_GetNextFromSeq(t *testing.T) {
	Convey("Get item at position next to 3", t, func() {
		initList(t)
		entry, err := list.GetNextFromSeq(3)
		So(err, ShouldBeNil)
		So(entry, ShouldEqual, "item 4")

	})
}

func TestListDB_MaxIndex(t *testing.T) {
	Convey("Get max index", t, func() {
		initList(t)
		index, err := list.MaxIndex()
		So(err, ShouldBeNil)
		So(index, ShouldEqual, 4)
	})
}

func TestListDB_MinIndex(t *testing.T) {
	Convey("Get min index", t, func() {
		initList(t)
		index, err := list.MaxIndex()
		So(err, ShouldBeNil)
		So(index, ShouldEqual, 1)
	})
}

func TestListDB_Delete(t *testing.T) {
	Convey("Delete item at 1", t, func() {
		initList(t)
		err := list.Delete(1)
		So(err, ShouldBeNil)

		count, err := list.Count()
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 3)

		index, err := list.MinIndex()
		So(err, ShouldBeNil)
		So(index, ShouldEqual, 2)
	})
}

func TestListDB_Clear(t *testing.T) {
	Convey("Clear list", t, func() {
		initList(t)
		err := list.Clear()
		So(err, ShouldBeNil)

		count, err := list.Count()
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 0)
	})
}

func TestListDB_Close(t *testing.T) {
	Convey("Close list", t, func() {
		initList(t)
		err := list.Close()
		So(err, ShouldBeNil)
	})
}
