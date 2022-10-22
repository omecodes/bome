package bome

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	dMap    *DMap
	person1 = DoubleMapEntry{
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
	if dMap == nil {
		var err error
		db, err := sql.Open(testDialect, testDBPath)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		_, err = db.Exec("drop table if exists jd_map;")
		So(err, ShouldBeNil)

		dMap, err = Build().SetConn(db).SetDialect("unsupported").SetTableName("jd_map").DMap()
		So(err, ShouldNotBeNil)
		So(dMap, ShouldBeNil)

		dMap, err = Build().SetConn(db).SetDialect(testDialect).SetTableName("jd_map").DMap()
		So(err, ShouldBeNil)
		So(dMap, ShouldNotBeNil)
	}
}

func TestNewJSONDoubleMapDB(t *testing.T) {
	Convey("Init database", t, func() {
		initJsonDoubleDbMap()
	})
}

func TestDMap_Save(t *testing.T) {
	Convey("Save double map entries", t, func() {
		initJsonDoubleDbMap()
		var err error
		err = dMap.Save(person1.FirstKey, person1.SecondKey, person1.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dMap.Save(person2.FirstKey, person2.SecondKey, person2.Value, SaveOptions{})
		So(err, ShouldBeNil)

		err = dMap.Save(person3.FirstKey, person3.SecondKey, person3.Value, SaveOptions{})
		So(err, ShouldBeNil)
	})
}

func TestDMap_Read(t *testing.T) {
	Convey("Read double map entries", t, func() {
		initJsonDoubleDbMap()
		var err error
		_, err = dMap.ReadRaw("people", "zebou")
		So(err, ShouldBeNil)

		_, err = dMap.ReadRaw("people", "wassiath")
		So(err, ShouldBeNil)

		_, err = dMap.ReadRaw("people", "akam")
		So(err, ShouldBeNil)
	})
}

func TestDMap_EditAt(t *testing.T) {
	Convey("EditAt item", t, func() {
		initJsonDoubleDbMap()
		err := dMap.Edit("people", "akam", "$.address.commune", StringExpr("yahou"))
		So(err, ShouldBeNil)

		value, err := dMap.String("people", "akam", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "yahou")
	})
}

func TestDMap_EditAll(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		initJsonDoubleDbMap()
		err := dMap.EditAllAt("$.address.country", StringExpr("CotedIvoire"))
		So(err, ShouldBeNil)

		c, err := dMap.Where(JsonAtEq("$.address.country", StringExpr("CotedIvoire")))
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		defer func() {
			_ = c.Close()
		}()

		var entries []interface{}
		var value interface{}
		for c.HasNext() {
			value, err = c.Entry()
			So(err, ShouldBeNil)
			entries = append(entries, value)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestDMap_EditAllMatching(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		initJsonDoubleDbMap()

		err := dMap.EditAt("$.family",
			JsonExpr(StringExpr("class"), StringExpr("youngsters")),
			JsonAtLt("$.age", IntExpr(30)))
		So(err, ShouldBeNil)

		c, err := dMap.Where(JsonAtEq("$.family.class", StringExpr("youngsters")))
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		defer func() {
			_ = c.Close()
		}()

		var value string
		var entries []string
		for c.HasNext() {
			value, err = c.Value()
			So(err, ShouldBeNil)
			entries = append(entries, value)
		}
		So(entries, ShouldHaveLength, 2)
	})
}

func TestDMap_ExtractAllString(t *testing.T) {
	Convey("Extract string values", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dMap.StringAt("$.address.commune", JsonAtEq("$.address.city", StringExpr("Abidjan")))
		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		for cursor.HasNext() {
			o, err := cursor.Entry()
			So(err, ShouldBeNil)
			So(o, ShouldBeIn, "Bingerville", "Yopougon")
		}
	})
}

func TestDMap_ExtractAllInt(t *testing.T) {
	Convey("Extract int values", t, func() {
		initJsonDoubleDbMap()
		c, err := dMap.IntAt("$.age", JsonAtEq("$.family.class", StringExpr("youngsters")))
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		defer func() {
			_ = c.Close()
		}()

		for c.HasNext() {
			o, err := c.Entry()
			So(err, ShouldBeNil)
			So(o, ShouldBeLessThan, 30)
		}
	})
}

func TestDMap_ExtractAt(t *testing.T) {
	Convey("EditAllAt item", t, func() {
		initJsonDoubleDbMap()
		value, err := dMap.String("people", "zebou", "$.address.commune")
		So(err, ShouldBeNil)
		So(value, ShouldEqual, "Bingerville")
	})
}

func TestDMap_SearchContainsPath(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		c, err := dMap.Where(JsonContainsPath("$.address.commune"))

		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		defer func() {
			_ = c.Close()
		}()

		var item interface{}
		var entries []*DoubleMapEntry
		for c.HasNext() {
			item, err = c.Entry()
			So(err, ShouldBeNil)

			entry := item.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestDMap_SearchAtContains(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dMap.Where(
			JsonAtContains("$.address.region", StringExpr("lagunes")),
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Entry()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 3)
	})
}

func TestDMap_Range(t *testing.T) {
	Convey("Get range", t, func() {
		initJsonDoubleDbMap()
		cursor, err := dMap.RangeOf(
			JsonAtContains("$.address.region", StringExpr("lagunes")), DoubleMapEntryScanner, 0, 2,
		)

		So(err, ShouldBeNil)
		So(cursor, ShouldNotBeNil)
		defer func() {
			_ = cursor.Close()
		}()

		var entries []*DoubleMapEntry
		for cursor.HasNext() {
			o, err := cursor.Entry()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 2)
	})
}

func TestDMap_SearchAtGt(t *testing.T) {
	Convey("Search in json all people with identified 'commune'", t, func() {
		initJsonDoubleDbMap()
		c, err := dMap.Where(
			JsonAtGt("$.age", IntExpr(29)),
		)

		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		defer func() {
			_ = c.Close()
		}()

		var entries []*DoubleMapEntry
		for c.HasNext() {
			o, err := c.Entry()
			So(err, ShouldBeNil)

			entry := o.(*DoubleMapEntry)
			entries = append(entries, entry)
		}
		So(entries, ShouldHaveLength, 1)
	})
}

func TestDMap_Clear(t *testing.T) {
	Convey("Clear all entries", t, func() {
		// initJsonDoubleDbMap()
		// err := dMap.Clear()
		// So(err, ShouldBeNil)
	})
}
