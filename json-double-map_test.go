package bome

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	dbJsonDoubleMap *JSONDoubleMap
	person1         = DoubleMapEntry{
		FirstKey:  "people",
		SecondKey: "zebou",
		Value:     doc1,
	}
	person2 = DoubleMapEntry{
		FirstKey:  "people",
		SecondKey: "wassiath",
		Value:     doc2,
	}
	person3 = DoubleMapEntry{
		FirstKey:  "people",
		SecondKey: "akam",
		Value:     doc3,
	}
)

func init() {
	_ = os.Remove(testDBPath)
}

func initJsonDoubleDbMap() {
	if dbJsonDoubleMap == nil {
		var err error
		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists jd_map;")
		So(err, ShouldBeNil)

		builder := &Builder{}

		dbJsonDoubleMap, err = builder.SetConn(db).SetDialect("unsupported").SetTableName("jd_map").JSONDoubleMap()
		So(err, ShouldNotBeNil)
		So(dbJsonDoubleMap, ShouldBeNil)

		dbJsonDoubleMap, err = builder.SetConn(db).SetDialect(testDialect).SetTableName("jd_map").JSONDoubleMap()
		So(err, ShouldBeNil)
		So(dbJsonDoubleMap, ShouldNotBeNil)
	}
}

func TestNewJSONDoubleMapDB(t *testing.T) {
	Convey("Init database", t, func() {
		initJsonDoubleDbMap()
	})
}

func TestJsonDoubleMap_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initJsonDoubleDbMap()
		var err error
		err = dbJsonDoubleMap.Save(&person1)
		So(err, ShouldBeNil)

		err = dbJsonDoubleMap.Save(&person2)
		So(err, ShouldBeNil)

		err = dbJsonDoubleMap.Save(&person3)
		So(err, ShouldBeNil)
	})
}

func TestJsonDoubleMap_EditAt(t *testing.T) {
	Convey("Edit item", t, func() {
		initJsonDoubleDbMap()
		err := dbJsonDoubleMap.EditAt("people", "akam", "$.address.commune", "yahou")
		So(err, ShouldBeNil)

		value, err := dbJsonDoubleMap.ExtractAt("people", "akam", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "yahou")
	})
}

func TestJsonDoubleMap_EditAll(t *testing.T) {
	Convey("Edit item", t, func() {
		initJsonDoubleDbMap()
		err := dbJsonDoubleMap.EditAll("$.address.country", StringExpr("CoteIvoire"))
		So(err, ShouldBeNil)

		cursor, err := dbJsonDoubleMap.Search(JsonAtEq("$.address.country", StringExpr("CoteIvoire")), DoubleMapEntryScanner)
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestJsonDoubleMap_EditAllMatching(t *testing.T) {
	Convey("Edit item", t, func() {
		initJsonDoubleDbMap()
		err := dbJsonDoubleMap.EditAllMatching("$.family", JsonExpr(StringExpr("class"), StringExpr("youngsters")),
			JsonAtLt("$.age", IntExpr(30)),
		)
		So(err, ShouldBeNil)

		cursor, err := dbJsonDoubleMap.Search(JsonAtEq("$.family.class", StringExpr("youngsters")), DoubleMapEntryScanner)
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 2)
	})
}

func TestJsonDoubleMap_ExtractAllString(t *testing.T) {
	Convey("Extract string values", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.ExtractAll("$.address.commune", JsonAtEq("$.address.city", StringExpr("Abidjan")), StringScanner)
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			So(o, ShouldBeIn, "Bingerville", "Yopougon")
		}
	})
}

func TestJsonDoubleMap_ExtractAllInt(t *testing.T) {
	Convey("Extract int values", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.ExtractAll("$.age", JsonAtEq("$.family.class", StringExpr("youngsters")), IntScanner)
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)
			So(o, ShouldBeLessThan, 30)
		}
	})
}

func TestJsonDoubleMap_ExtractAt(t *testing.T) {
	Convey("Edit item", t, func() {
		initJsonDoubleDbMap()
		value, err := dbJsonDoubleMap.ExtractAt("people", "zebou", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "Bingerville")
	})
}

func TestJsonDoubleMap_SearchContainsPath(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.Search(
			JsonContainsPath("$.address.commune"), DoubleMapEntryScanner,
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestJsonDoubleMap_SearchAtContains(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.Search(
			JsonAtContains("$.address.region", StringExpr("lagunes")), DoubleMapEntryScanner,
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestJsonDoubleMap_Range(t *testing.T) {
	Convey("Get range", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.RangeOf(
			JsonAtContains("$.address.region", StringExpr("lagunes")), DoubleMapEntryScanner, 0, 2,
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 2)
	})
}

func TestJsonDoubleMap_SearchAtGt(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dbJsonDoubleMap.Search(
			JsonAtGt("$.age", IntExpr(29)), DoubleMapEntryScanner,
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Next()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 1)
	})
}

func TestJsonDoubleMap_Clear(t *testing.T) {
	Convey("Clear all entries", t, func() {
		initJsonDoubleDbMap()
		err := dbJsonDoubleMap.Clear()
		So(err, ShouldBeNil)
	})
}
