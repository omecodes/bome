package bome

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/xwb1989/sqlparser"
	"testing"
)

func testExpression(t *testing.T, ex string) {
	rawEx := RawExpr(fmt.Sprintf("select * from t where %s", ex))
	_, err := sqlparser.Parse(rawEx.eval())
	So(err, ShouldBeNil)
}

func TestStringExpr(t *testing.T) {
	Convey("StringExpr", t, func() {
		str := StringExpr("val").eval()
		So(str, ShouldEqual, "'val'")
	})
}

func TestIntExpr(t *testing.T) {
	Convey("IntExpr", t, func() {
		str := IntExpr(23).eval()
		So(str, ShouldEqual, "23")
	})
}

func TestEq(t *testing.T) {
	Convey("Eq", t, func() {
		e := Eq(StringExpr("val"))
		testExpression(t, e.sql())
	})
}

func TestGt(t *testing.T) {
	Convey("Gt", t, func() {
		e := Gt(IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestContains(t *testing.T) {
	Convey("Contains", t, func() {
		e := Contains(StringExpr("text"))
		testExpression(t, e.sql())
	})
}

func TestGte(t *testing.T) {
	Convey("Gte", t, func() {
		e := Gte(IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestJsonAtContains(t *testing.T) {
	Convey("JsonAtContains", t, func() {
		e := JsonAtContains("$.tests.unit", StringExpr("pattern"))
		testExpression(t, e.sql())
	})
}

func TestJsonAtEndsWith(t *testing.T) {
	Convey("JsonAtEndsWith", t, func() {
		e := JsonAtEndsWith("$.tests.unit", StringExpr("pattern"))
		testExpression(t, e.sql())
	})
}

func TestJsonContains(t *testing.T) {
	Convey("JsonContains", t, func() {
		e := JsonContainsPath("$.item.at.path")
		testExpression(t, e.sql())
	})
}

func TestLt(t *testing.T) {
	Convey("Lt", t, func() {
		e := Lt(IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestLte(t *testing.T) {
	Convey("Lte", t, func() {
		e := Lte(IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestJsonAtEq(t *testing.T) {
	Convey("JsonAtEq", t, func() {
		e := JsonAtEq("$.json.item.path", StringExpr("val"))
		testExpression(t, e.sql())

		e = JsonAtEq("$.json.item.path", IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestJsonAtGe(t *testing.T) {
	Convey("JsonAtGe", t, func() {
		e := JsonAtGe("$.json.int.at.path", IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestJsonAtGt(t *testing.T) {
	Convey("JsonAtGt", t, func() {
		e := JsonAtGt("$.json.int.at.path", IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestNe(t *testing.T) {
	Convey("Ne", t, func() {
		e := Ne(IntExpr(23))
		testExpression(t, e.sql())

		e = Ne(StringExpr("val"))
		testExpression(t, e.sql())
	})
}

func TestJsonAtLe(t *testing.T) {
	Convey("JsonAtLe", t, func() {
		e := JsonAtLe("$.json.int.at.path", IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestNot(t *testing.T) {
	Convey("Not", t, func() {
		e := Not(Eq(StringExpr("val")))
		testExpression(t, e.sql())
	})
}

func TestJsonAtStartsWith(t *testing.T) {
	Convey("JsonAtStartsWith", t, func() {
		e := JsonAtStartsWith("$.json.int.at.path", StringExpr("return"))
		testExpression(t, e.sql())
	})
}

func TestJsonAtLt(t *testing.T) {
	Convey("JsonAtLt", t, func() {
		e := JsonAtLt("$.json.int.at.path", IntExpr(23))
		testExpression(t, e.sql())
	})
}

func TestRawExpr(t *testing.T) {
	Convey("RawExpr", t, func() {})
}

func TestAnd(t *testing.T) {
	Convey("And", t, func() {
		e := And(Eq(StringExpr("")), StartsWith(StringExpr("str2")))
		testExpression(t, e.sql())
	})
}

func TestOr(t *testing.T) {
	Convey("Or", t, func() {
		e := Or(Contains(StringExpr("er")), Contains(StringExpr("e")))
		testExpression(t, e.sql())
	})
}

func TestStartsWith(t *testing.T) {
	Convey("And", t, func() {
		e := StartsWith(StringExpr("str2"))
		testExpression(t, e.sql())
	})
}

func TestEndsWith(t *testing.T) {
	Convey("And", t, func() {
		e := EndsWith(StringExpr("str2"))
		testExpression(t, e.sql())
	})
}
