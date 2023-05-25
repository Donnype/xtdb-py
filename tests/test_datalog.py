import pytest

from xtdb.datalog import Aggregate, Expression, Find, Where
from xtdb.exceptions import XTDBException


def test_where_clauses():
    statement = Where("a", "b", "c")
    assert statement.compile() == "[ a :b c ]"

    statement = Where("a", "b", "c") & Where("1", "2", "3")
    assert statement.compile() == "[ 1 :2 3 ] [ a :b c ]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("1", "2", "3")
    assert statement.compile() == "[ 1 :2 3 ] [ a :b c ]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("x", "y", "z")
    assert statement.compile() == "[ 1 :2 3 ] [ a :b c ] [ x :y z ]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("1", "2", "3") & Where("a", "b", "c")
    assert statement.compile() == "[ 1 :2 3 ] [ a :b c ]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("x", "y", "z")
    assert statement.compile() == "[ 1 :2 3 ] [ a :b c ] [ x :y z ]"
    assert str(statement) == statement.compile()


def test_or_clauses():
    statement = Where("a", "b", "c") | Where("1", "2", "3")
    assert statement.compile() == "(or [ 1 :2 3 ] [ a :b c ])"

    statement = Where("a", "b", "c") | Where("1", "2", "3") | Where("1", "2", "3")
    assert statement.compile() == "(or [ 1 :2 3 ] [ a :b c ])"


def test_where_or_clauses():
    statement = Where("a", "b", "c") & Where("1", "2", "3") | Where("x", "y", "z")
    assert statement.compile() == "(or [ x :y z ]) [ 1 :2 3 ] [ a :b c ]"

    statement = Where("a", "b", "c") & (Where("1", "2", "3") | Where("x", "y", "z"))
    assert statement.compile() == "(or [ 1 :2 3 ] [ x :y z ]) [ a :b c ]"

    # The & operator takes precedence over the | operator
    statement = (Where("a", "b", "c") | Where("1", "2", "3")) & Where("x", "y", "z")
    assert statement.compile() == "(or [ 1 :2 3 ] [ a :b c ]) [ x :y z ]"

    statement = Where("a", "b", "c") | Where("1", "2", "3") & Where("x", "y", "z")
    assert statement.compile() == "(or [ a :b c ]) [ 1 :2 3 ] [ x :y z ]"

    statement = (Where("a", "b", "c") | Where("1", "2", "3")) & (Where("x", "y", "z") | Where("9", "8", "7"))
    assert statement.compile() == "(or [ 1 :2 3 ] [ a :b c ]) (or [ 9 :8 7 ] [ x :y z ])"


def test_find_clauses():
    statement = Find("a")
    assert statement.compile() == "a"

    statement = Find("a") & Find("b")
    assert statement.compile() == "a b"

    statement = Find("pull(*)") & Find("b") & Find(Expression("(sum ?heads)"))
    assert statement.compile() == "pull(*) b (sum ?heads)"

    with pytest.raises(XTDBException) as ctx:
        Find("pull(*)") | Find("b")

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Or operator is not supported for find clauses"


def test_aggregates():
    statement = Find("a") & Find(Aggregate("sum", "field"))
    assert statement.compile() == "a (sum field)"

    statement = Find("a") & Find(Aggregate("sample", "field", "12"))
    assert statement.compile() == "a (sample 12 field)"

    with pytest.raises(XTDBException) as ctx:
        Find("a") & Find(Aggregate("wrong", "field"))

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Invalid aggregate function"

    with pytest.raises(XTDBException) as ctx:
        Find("a") & Find(Aggregate("rand", "field"))

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Invalid arguments to aggregate function, needs: ('N',)"
