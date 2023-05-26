import pytest

from tests.conftest import FirstEntity, SecondEntity
from xtdb.query import InvalidField, Query


def test_basic_field_where_clause():
    query = Query(FirstEntity).where(FirstEntity, name="test")
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :FirstEntity/name "test" ]
    [ FirstEntity :type "FirstEntity" ]]}}"""
    )

    query = query.limit(4)
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :FirstEntity/name "test" ]
    [ FirstEntity :type "FirstEntity" ]] :limit 4}}"""
    )

    query = query.offset(0)
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :FirstEntity/name "test" ]
    [ FirstEntity :type "FirstEntity" ]] :limit 4 :offset 0}}"""
    )

    query = query.timeout(40)
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :FirstEntity/name "test" ]
    [ FirstEntity :type "FirstEntity" ]] :limit 4 :offset 0 :timeout 40}}"""
    )


def test_reference_field_where_clause():
    query = Query(FirstEntity).where(SecondEntity, first_entity=FirstEntity)
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :type "FirstEntity" ]
    [ SecondEntity :SecondEntity/first_entity FirstEntity ]]}}"""
    )


def test_remove_duplicates():
    query = Query(FirstEntity).where(SecondEntity, first_entity=FirstEntity)
    assert query == query.where(SecondEntity, first_entity=FirstEntity)


def test_invalid_fields_name():
    with pytest.raises(InvalidField) as ctx:
        Query(FirstEntity).where(FirstEntity, wrong=FirstEntity)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "wrong" is not a field of FirstEntity'

    with pytest.raises(InvalidField) as ctx:
        Query(FirstEntity).where(FirstEntity, abc="def")

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "abc" is not a field of FirstEntity'


def test_escaping_quotes():
    query = Query(FirstEntity).where(SecondEntity, first_entity=FirstEntity).where(FirstEntity, name='test " name')
    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :FirstEntity/name "test \\" name" ]
    [ FirstEntity :type "FirstEntity" ]
    [ SecondEntity :SecondEntity/first_entity FirstEntity ]]}}"""
    )


def test_invalid_field_types():
    with pytest.raises(InvalidField) as ctx:
        Query(FirstEntity).where(SecondEntity, test=InvalidField)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "test" is not a field of SecondEntity'

    with pytest.raises(InvalidField) as ctx:
        Query(FirstEntity).where(FirstEntity, name=FirstEntity)

    assert ctx.exconly() == 'xtdb.exceptions.InvalidField: "name" is not a relation of FirstEntity'


def test_allow_string_for_foreign_keys():
    query = Query(FirstEntity).where(SecondEntity, first_entity="FirstEntity|internet")

    assert (
        query.format()
        == """{:query {:find [(pull FirstEntity [*])] :where [
    [ FirstEntity :type "FirstEntity" ]
    [ SecondEntity :SecondEntity/first_entity "FirstEntity|internet" ]]}}"""
    )
