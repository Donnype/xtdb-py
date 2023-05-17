import pytest

from tests.conftest import SecondEntity, TestEntity
from xtdb.query import InvalidField, Query


def test_basic_field_where_clause():
    query = Query(TestEntity).where(TestEntity, name="test")
    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ TestEntity :TestEntity/name "test" ]
    [ TestEntity :type "TestEntity" ]]}}"""
    )

    query = query.limit(4)
    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ TestEntity :TestEntity/name "test" ]
    [ TestEntity :type "TestEntity" ]] :limit 4}}"""
    )
    query = query.offset(0)
    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ TestEntity :TestEntity/name "test" ]
    [ TestEntity :type "TestEntity" ]] :limit 4 :offset 0}}"""
    )


def test_reference_field_where_clause():
    query = Query(TestEntity).where(SecondEntity, test_entity=TestEntity)
    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ SecondEntity :SecondEntity/test_entity TestEntity ]
    [ TestEntity :type "TestEntity" ]]}}"""
    )


def test_remove_duplicates():
    query = Query(TestEntity).where(SecondEntity, test_entity=TestEntity)
    assert query == query.where(SecondEntity, test_entity=TestEntity)


def test_invalid_fields_name():
    with pytest.raises(InvalidField) as ctx:
        Query(TestEntity).where(TestEntity, wrong=TestEntity)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "wrong" is not a field of TestEntity'

    with pytest.raises(InvalidField) as ctx:
        Query(TestEntity).where(TestEntity, abc="def")

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "abc" is not a field of TestEntity'


def test_escaping_quotes():
    query = Query(TestEntity).where(SecondEntity, test_entity=TestEntity).where(TestEntity, name='test " name')
    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ SecondEntity :SecondEntity/test_entity TestEntity ]
    [ TestEntity :TestEntity/name "test \\" name" ]
    [ TestEntity :type "TestEntity" ]]}}"""
    )


def test_invalid_field_types():
    with pytest.raises(InvalidField) as ctx:
        Query(TestEntity).where(SecondEntity, test=InvalidField)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "test" is not a field of SecondEntity'

    with pytest.raises(InvalidField) as ctx:
        Query(TestEntity).where(TestEntity, name=TestEntity)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "name" is not a relation of TestEntity'


def test_allow_string_for_foreign_keys():
    query = Query(TestEntity).where(SecondEntity, test_entity="TestEntity|internet")

    assert (
        query.format()
        == """{:query {:find [(pull TestEntity [*])] :where [
    [ SecondEntity :SecondEntity/test_entity "TestEntity|internet" ]
    [ TestEntity :type "TestEntity" ]]}}"""
    )
