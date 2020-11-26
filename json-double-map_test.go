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

func initJsonDoubleDbMap(t *testing.T) {
	if dbJsonDoubleMap == nil {
		var err error
		db, err = sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists jd_map;")
		So(err, ShouldBeNil)

		dbJsonDoubleMap, err = NewJSONDoubleMap(db, "unsupported", "d_map")
		So(err, ShouldNotBeNil)
		So(dbJsonDoubleMap, ShouldBeNil)

		dbJsonDoubleMap, err = NewJSONDoubleMap(db, testDialect, "jd_map")
		So(err, ShouldBeNil)
		So(dbJsonDoubleMap, ShouldNotBeNil)
	}
}

func TestNewJSONDoubleMapDB(t *testing.T) {
	Convey("Init database", t, func() {
		initJsonDoubleDbMap(t)
	})
}

func TestJsonDoubleMap_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initJsonDoubleDbMap(t)

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
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			err := dbJsonDoubleMap.EditAt("people", "akam", "$.address.commune", "yahou")
			So(err, ShouldBeNil)

			value, err := dbJsonDoubleMap.ExtractAt("people", "akam", "$.address.commune")
			So(err, ShouldBeNil)
			So(value, ShouldEqual, "yahou")
		})
	}
}

func TestJsonDoubleMap_EditAll(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			err := dbJsonDoubleMap.EditAll("$.address.country", StringExpr("Côte d'Ivoire"))
			So(err, ShouldBeNil)

			cursor, err := dbJsonDoubleMap.Search(JsonAtEq("$.address.country", StringExpr("Côte d'Ivoire")), DoubleMapEntryScanner)
			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_EditAllMatching(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			err := dbJsonDoubleMap.EditAllMatching("$.family", JsonExpr(StringExpr("class"), StringExpr("youngsters")),
				JsonAtLt("$.age", IntExpr(30)),
			)
			So(err, ShouldBeNil)

			cursor, err := dbJsonDoubleMap.Search(JsonAtEq("$.family.class", StringExpr("youngsters")), DoubleMapEntryScanner)
			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_ExtractAllString(t *testing.T) {
	if jsonTestEnabled {
		Convey("Extract string values", t, func() {
			cursor, err := dbJsonDoubleMap.ExtractAll("$.address.commune", JsonAtEq("$.address.city", StringExpr("Abidjan")), StringScanner)
			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

			for cursor.HasNext() {
				o, err := cursor.Next()
				So(err, ShouldBeNil)
				So(o, ShouldBeIn, "Bingerville", "Yopougon")
			}
		})
	}
}

func TestJsonDoubleMap_ExtractAllInt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Extract int values", t, func() {
			cursor, err := dbJsonDoubleMap.ExtractAll("$.age", JsonAtEq("$.family.class", StringExpr("youngsters")), IntScanner)
			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

			for cursor.HasNext() {
				o, err := cursor.Next()
				So(err, ShouldBeNil)
				So(o, ShouldBeLessThan, 30)
			}
		})
	}
}

func TestJsonDoubleMap_ExtractAt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Edit item", t, func() {
			value, err := dbJsonDoubleMap.ExtractAt("people", "zebou", "$.address.commune")
			So(err, ShouldBeNil)
			So(value, ShouldEqual, "Bingerville")
		})
	}
}

func TestJsonDoubleMap_SearchContainsPath(t *testing.T) {
	if jsonTestEnabled {
		Convey("Search in json all people with identified 'commune'", t, func() {
			cursor, err := dbJsonDoubleMap.Search(
				JsonContainsPath("$.address.commune"), DoubleMapEntryScanner,
			)

			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_SearchAtContains(t *testing.T) {
	if jsonTestEnabled {
		Convey("Search in json all people with identified 'commune'", t, func() {
			cursor, err := dbJsonDoubleMap.Search(
				JsonAtContains("$.address.region", StringExpr("lagunes")), DoubleMapEntryScanner,
			)

			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_Range(t *testing.T) {
	if jsonTestEnabled {
		Convey("Get range", t, func() {
			cursor, err := dbJsonDoubleMap.RangeOf(
				JsonAtContains("$.address.region", StringExpr("lagunes")), DoubleMapEntryScanner, 0, 2,
			)

			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_SearchAtGt(t *testing.T) {
	if jsonTestEnabled {
		Convey("Search in json all people with identified 'commune'", t, func() {
			cursor, err := dbJsonDoubleMap.Search(
				JsonAtGt("$.age", IntExpr(29)), DoubleMapEntryScanner,
			)

			So(err, ShouldBeNil)
			So(cursor, ShouldNotBeNil)
			defer cursor.Close()

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
}

func TestJsonDoubleMap_Clear(t *testing.T) {
	if jsonTestEnabled {
		Convey("Clear all entries", t, func() {
			err := dbJsonDoubleMap.Clear()
			So(err, ShouldBeNil)
		})
	}
}
