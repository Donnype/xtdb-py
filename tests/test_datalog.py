import pytest

from xtdb.datalog import (
    Expression,
    Find,
    In,
    Limit,
    NotJoin,
    OrderBy,
    OrJoin,
    Sample,
    Sum,
    Timeout,
    Where,
    WherePredicate,
    _BaseAggregate,
)
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


def test_not_clauses():
    statement = ~Where("a", "b", "c")
    assert statement.compile() == ":where [(not [ a :b c ])]"

    statement = ~(Where("a", "b", "c") & Where("1", "2", "3"))
    assert statement.compile() == ":where [(not [ 1 :2 3 ] [ a :b c ])]"

    statement = ~(Where("a", "b", "c") & Where("1", "2", "3") & Where("x", "y"))
    assert statement.compile() == ":where [(not [ 1 :2 3 ] [ a :b c ] [ x :y  ])]"


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

    statement = Find("a") & (Where("a", "b", "c") | Where("1", "2", "3"))
    assert statement.compile() == "{:query {:find [a] :where [(or [ 1 :2 3 ] [ a :b c ])]}}"

    statement = Find("a") & Find("b") & (Where("a", "b", "c") | Where("1", "2", "3"))
    assert statement.compile() == "{:query {:find [ a b] :where [(or [ 1 :2 3 ] [ a :b c ])]}}"

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


def test_find_where_not():
    statement = Sum("a") & Sum("b") & ~(Where("a", "b", "c") & Where("x", "y", "z"))
    assert statement.compile() == "{:query {:find [ (sum a) (sum b)] :where [(not [ a :b c ] [ x :y z ])]}}"


def test_find_where_in_complete():
    statement = Find("a") & Where("a", "b", "c") & Limit(2) & Timeout(29) & OrderBy([("b", "asc")]) & In("c", "d")

    # Scalar binding
    assert (
        statement.compile()
        == '{:query {:find [a] :where [[ a :b c ]] :in [c] :order-by [[b :asc]] :limit 2 :timeout 29} :in-args ["d"]}'
    )

    # Collection binding
    statement = Find("a") & Where("a", "b", "c") & Limit(2) & OrderBy([("b", "asc")]) & In(["c", "..."], ["d", "e"])
    assert (
        statement.compile()
        == '{:query {:find [a] :where [[ a :b c ]] :in [[c ...]] :order-by [[b :asc]] :limit 2} :in-args [["d" "e"]]}'
    )

    # Tuple binding
    statement = Find("a") & Where("a", "b", "c") & Limit(2) & OrderBy([("b", "asc")]) & In(["c", "z"], ["d", "e"])
    assert (
        statement.compile()
        == '{:query {:find [a] :where [[ a :b c ]] :in [[c z]] :order-by [[b :asc]] :limit 2} :in-args [["d" "e"]]}'
    )

    # Relation binding
    statement = Find("a") & Where("a", "b", "c") & Limit(2) & In([["c", "z"]], [["d", "e"], ["f", "g"]])
    assert (
        statement.compile()
        == '{:query {:find [a] :where [[ a :b c ]] :in [[[c z]]] :limit 2} :in-args [[["d" "e"] ["f" "g"]]]}'
    )


def test_find_where_wrong_order():
    with pytest.raises(XTDBException):
        Find("a") & Where("a", "b", "c") & Where("1", "2", "3")

    with pytest.raises(XTDBException):
        Where("a", "b", "c") & Find("a")

    with pytest.raises(XTDBException):
        (Where("a", "b", "c") | Where("1", "2", "3")) & Find("a")


def test_in():
    statement = In(["field", "other-field"], ["value", "other-value"])
    assert statement.compile() == " :in [[field other-field]]"
    assert statement.compile_values() == ' :in-args [["value" "other-value"]]'

    statement = In(["field", "..."], ["value", "other-value"])
    assert statement.compile() == " :in [[field ...]]"
    assert statement.compile_values() == ' :in-args [["value" "other-value"]]'


def test_order_by():
    statement = OrderBy([("field_name", "asc"), ("test-name", "desc")])
    assert statement.compile() == " :order-by [[field_name :asc] [test-name :desc]]"

    with pytest.raises(XTDBException):
        OrderBy([("field_name", "asc"), ("test-name", "esc")])


def test_where_predicate():
    statement = WherePredicate("odd?", "b")
    assert statement.compile() == ":where [[ (odd? b) ]]"

    statement = WherePredicate("+", "1", "2", "b")
    assert statement.compile() == ":where [[ (+ 1 2 b) ]]"

    # From the docs
    statement = WherePredicate("identity", "2", bind="x") & WherePredicate("+", "x", "2", bind="y")
    assert statement.compile() == ":where [ [ (+ x 2) y] [ (identity 2) x]]"

    statement = Find("a") & (Where("a", "b", "c") & WherePredicate("odd?", "c"))
    assert statement.compile() == "{:query {:find [a] :where [ [ (odd? c) ] [ a :b c ]]}}"


def test_range_predicate():
    # From the docs
    statement = WherePredicate(">", 18, "a")
    assert statement.compile() == ":where [[ (> 18 a) ]]"


def test_unification_predicate():
    # From the docs
    statement = WherePredicate("==", "a", "a2")
    assert statement.compile() == ":where [[ (== a a2) ]]"

    # From the docs
    statement = WherePredicate("!=", "a", "a2")
    assert statement.compile() == ":where [[ (!= a a2) ]]"


def test_not_join():
    # From the docs
    statement = Where("e", "xt/id") & (NotJoin("e") & Where("e", "last-name", "n"))
    assert statement.compile() == ":where [ (not-join [e] [ e :last-name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & (NotJoin("e") & (Where("e", "last-name", "n") & Where("e", "name", "n")))
    assert statement.compile() == ":where [ (not-join [e]  [ e :last-name n ] [ e :name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & (NotJoin("e") & Where("e", "last-name", "n") & Where("e", "name", "n"))
    assert statement.compile() == ":where [ (not-join [e] [ e :last-name n ] [ e :name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & NotJoin("e") & Where("e", "last-name", "n") & Where("e", "name", "n")
    assert statement.compile() == ":where [ (not-join [e] ) [ e :last-name n ] [ e :name n ] [ e :xt/id  ]]"


def test_or_join():
    statement = Where("e", "xt/id") & (OrJoin("e") & Where("e", "last-name", "n"))
    assert statement.compile() == ":where [ (or-join [e] [ e :last-name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & (OrJoin("e") & (Where("e", "last-name", "n") & Where("e", "name", "n")))
    assert statement.compile() == ":where [ (or-join [e]  [ e :last-name n ] [ e :name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & (OrJoin("e") & Where("e", "last-name", "n") & Where("e", "name", "n"))
    assert statement.compile() == ":where [ (or-join [e] [ e :last-name n ] [ e :name n ]) [ e :xt/id  ]]"

    statement = Where("e", "xt/id") & OrJoin("e") & Where("e", "last-name", "n") & Where("e", "name", "n")
    assert statement.compile() == ":where [ (or-join [e] ) [ e :last-name n ] [ e :name n ] [ e :xt/id  ]]"
