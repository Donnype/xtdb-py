from xtdb.datalog import Where


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
