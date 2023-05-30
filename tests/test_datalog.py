import pytest

from xtdb.datalog import Expression, Find, Sample, Sum, Where, _BaseAggregate
from xtdb.exceptions import XTDBException


def test_where_clauses():
    statement = Where("a", "b", "c")
    assert statement.compile() == ":where [[ a :b c ]]"

    statement = Where("a", "b")
    assert statement.compile() == ":where [[ a :b  ]]"

    statement = Where("a", "b", "c") & Where("1", "2", "3")
    assert statement.compile() == ":where [ [ 1 :2 3 ] [ a :b c ]]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("1", "2", "3")
    assert statement.compile() == ":where [ [ 1 :2 3 ] [ a :b c ]]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("x", "y", "z")
    assert statement.compile() == ":where [ [ 1 :2 3 ] [ a :b c ] [ x :y z ]]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("1", "2", "3") & Where("a", "b", "c")
    assert statement.compile() == ":where [ [ 1 :2 3 ] [ a :b c ]]"

    statement = Where("a", "b", "c") & Where("1", "2", "3") & Where("x", "y", "z")
    assert statement.compile() == ":where [ [ 1 :2 3 ] [ a :b c ] [ x :y z ]]"
    assert str(statement) == statement.compile()


def test_or_clauses():
    statement = Where("a", "b", "c") | Where("1", "2", "3")
    assert statement.compile() == ":where [(or [ 1 :2 3 ] [ a :b c ])]"

    statement = Where("a", "b", "c") | Where("1", "2", "3") | Where("1", "2", "3")
    assert statement.compile() == ":where [(or [ 1 :2 3 ] [ a :b c ])]"


def test_where_or_clauses():
    statement = Where("a", "b", "c") & Where("1", "2", "3") | Where("x", "y", "z") & Where("9", "8", "7")
    assert statement.compile() == ":where [(or (and [ 1 :2 3 ] [ a :b c ]) (and [ 9 :8 7 ] [ x :y z ]))]"

    statement = Where("a", "b", "c") & (Where("1", "2", "3") | Where("x", "y", "z"))
    assert statement.compile() == ":where [ (or [ 1 :2 3 ] [ x :y z ]) [ a :b c ]]"

    # The & operator takes precedence over the | operator
    statement = (Where("a", "b", "c") | Where("1", "2", "3")) & Where("x", "y", "z")
    assert statement.compile() == ":where [ (or [ 1 :2 3 ] [ a :b c ]) [ x :y z ]]"

    statement = (Where("a", "b", "c") | Where("1", "2", "3")) & (Where("x", "y", "z") | Where("9", "8", "7"))
    assert statement.compile() == ":where [ (or [ 1 :2 3 ] [ a :b c ]) (or [ 9 :8 7 ] [ x :y z ])]"

    with pytest.raises(XTDBException):
        Where("a", "b", "c") & Where("1", "2", "3") | Where("x", "y", "z")

    with pytest.raises(XTDBException):
        Where("a", "b", "c") | Where("1", "2", "3") & Where("x", "y", "z")


def test_find_clauses():
    statement = Find("a")
    assert statement.compile() == ":find [a]"

    statement = Find("a") & Find("b")
    assert statement.compile() == ":find [ a b]"

    statement = Find("pull(*)") & Find("b") & Find(Expression("(sum ?heads)"))
    assert statement.compile() == ":find [ pull(*) b (sum ?heads)]"

    with pytest.raises(XTDBException) as ctx:
        Find("pull(*)") | Find("b")

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Cannot use | on query keys"


def test_aggregates():
    statement = Find("a") & Find(_BaseAggregate("sum", "field"))
    assert statement.compile() == ":find [ a (sum field)]"

    statement = Find("a") & Find(_BaseAggregate("sample", "field", "12"))
    assert statement.compile() == ":find [ a (sample 12 field)]"

    with pytest.raises(XTDBException) as ctx:
        Find("a") & Find(_BaseAggregate("wrong", "field"))

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Invalid aggregate function"

    with pytest.raises(XTDBException) as ctx:
        Find("a") & Find(_BaseAggregate("rand", "field"))

    assert ctx.exconly() == "xtdb.exceptions.XTDBException: Invalid arguments to aggregate, it needs one argument: N"


def test_concrete_aggregates():
    statement = Find("a") & Sum("field")
    assert statement.compile() == ":find [ a (sum field)]"

    statement = Find("a") & Sum("field") & Sum("field")
    assert statement.compile() == ":find [ a (sum field) (sum field)]"

    statement = Find("a") & Sample("field", 12)
    assert statement.compile() == ":find [ a (sample 12 field)]"

    with pytest.raises(XTDBException):
        Sample("field", 12) | Sum("field")


def test_find_where():
    statement = Find("a") & Where("a", "b", "c")
    assert statement.compile() == "{:query {:find [a] :where [[ a :b c ]]}}"

    statement = Find("a") & (Where("a", "b", "c") & Where("1", "2", "3"))
    assert statement.compile() == "{:query {:find [a] :where [ [ 1 :2 3 ] [ a :b c ]]}}"

    statement = (
        Find("a")
        & Sample("field", 12)
        & Sum("field")
        & ((Where("a", "b", "c") | Where("1", "2", "3")) & Where("x", "y", "z"))
    )
    assert (
        statement.compile()
        == "{:query {:find [ a (sample 12 field) (sum field)] :where [ (or [ 1 :2 3 ] [ a :b c ]) [ x :y z ]]}}"
    )

    statement = Sum("a") & Where("a", "b", "c")
    assert statement.compile() == "{:query {:find [(sum a)] :where [[ a :b c ]]}}"

    statement = Sum("a") & Sum("b") & Where("a", "b", "c")
    assert statement.compile() == "{:query {:find [ (sum a) (sum b)] :where [[ a :b c ]]}}"

    with pytest.raises(XTDBException):
        Find("a") & Where("a", "b", "c") & Where("1", "2", "3")

    with pytest.raises(XTDBException):
        Where("a", "b", "c") & Find("a")

    with pytest.raises(XTDBException):
        (Where("a", "b", "c") | Where("1", "2", "3")) & Find("a")
