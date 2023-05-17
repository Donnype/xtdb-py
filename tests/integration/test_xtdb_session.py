import os
from datetime import datetime

import pytest

from tests.conftest import FourthEntity, SecondEntity, TestEntity, ThirdEntity
from xtdb.query import Query
from xtdb.session import XTDBSession

if os.environ.get("CI") != "1":
    pytest.skip("Needs XTDB container.", allow_module_level=True)


def test_status(xtdb_session: XTDBSession):
    result = xtdb_session._client.status()
    assert result.version == "1.21.0"
    assert result.kvStore == "xtdb.rocksdb.RocksKv"


def test_query_no_results(xtdb_session: XTDBSession):
    query = Query(TestEntity).where(TestEntity, name="test")

    result = xtdb_session.query(query)
    assert result == []


def test_query_simple_filter(xtdb_session: XTDBSession):
    entity = TestEntity(name="test")
    xtdb_session.put(entity)

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result == []

    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="wrong")
    result = xtdb_session.query(query)
    assert result == []

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity._pk}]]

    xtdb_session.delete(entity)
    xtdb_session.commit()


def test_match(xtdb_session: XTDBSession, valid_time: datetime):
    entity = TestEntity(name="test")
    second_entity = TestEntity(name="test2")

    xtdb_session.put(entity)
    xtdb_session.put(second_entity)
    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity._pk}]]

    xtdb_session.delete(entity)
    xtdb_session.commit()

    third_entity = TestEntity(name="test3")

    xtdb_session.put(third_entity)
    xtdb_session.match(entity)  # transaction will fail because `entity` is not matched
    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="test3")
    assert xtdb_session.query(query) == []

    xtdb_session.put(third_entity)
    xtdb_session.match(second_entity)  # transaction will succeed because `second_entity` is matched
    xtdb_session.commit()

    assert xtdb_session.query(query) == [
        [{"TestEntity/name": "test3", "type": "TestEntity", "xt/id": third_entity._pk}]
    ]
    assert xtdb_session.query(query, valid_time) == []

    xtdb_session.delete(second_entity)
    xtdb_session.delete(third_entity)
    xtdb_session.commit()


def test_deleted_and_evicted(xtdb_session: XTDBSession, valid_time: datetime):
    entity = TestEntity(name="test")

    xtdb_session.put(entity, valid_time)
    xtdb_session.commit()

    xtdb_session.delete(entity)
    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result == []

    result = xtdb_session.query(query, valid_time)
    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity._pk}]]

    xtdb_session.evict(entity)
    xtdb_session.commit()

    result = xtdb_session.query(query)
    assert result == []

    result = xtdb_session.query(query, valid_time)
    assert result == []


def test_query_not_empty_on_reference_filter_for_entity(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second1 = SecondEntity(test_entity=test, age=1)
    second2 = SecondEntity(test_entity=test, age=2)

    xtdb_session.put(test)
    xtdb_session.put(second1)
    xtdb_session.put(second2)
    xtdb_session.commit()

    query = Query(TestEntity).where(SecondEntity, age=1).where(SecondEntity, test_entity=TestEntity)
    result = xtdb_session.query(query)

    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": test._pk}]]

    query = query.where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": test._pk}]]

    xtdb_session.delete(test)
    xtdb_session.delete(second1)
    xtdb_session.delete(second2)
    xtdb_session.commit()


def test_deep_query(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second = SecondEntity(test_entity=test, age=1)
    second2 = SecondEntity(test_entity=test, age=3)
    third = ThirdEntity(second_entity=second2, test_entity=test)
    fourth = FourthEntity(third_entity=third, value=15.3)

    xtdb_session.put(test)
    xtdb_session.put(second)
    xtdb_session.put(second2)
    xtdb_session.put(third)
    xtdb_session.put(fourth)
    xtdb_session.commit()

    query = Query(SecondEntity)
    result = xtdb_session.query(query)

    assert len(result) == 2
    assert [
        {"SecondEntity/age": 3, "type": "SecondEntity", "xt/id": second2._pk, "SecondEntity/test_entity": test._pk}
    ] in result
    assert [
        {"SecondEntity/age": 1, "type": "SecondEntity", "xt/id": second._pk, "SecondEntity/test_entity": test._pk}
    ] in result

    query = query.where(FourthEntity, third_entity=ThirdEntity, value=15.3).where(
        ThirdEntity, second_entity=SecondEntity
    )
    result = xtdb_session.query(query)
    assert result == [
        [{"SecondEntity/age": 3, "type": "SecondEntity", "xt/id": second2._pk, "SecondEntity/test_entity": test._pk}]
    ]

    xtdb_session.delete(fourth)
    xtdb_session.delete(third)
    xtdb_session.delete(second2)
    xtdb_session.delete(second)
    xtdb_session.delete(test)
    xtdb_session.commit()


def test_query_empty_on_reference_filter_for_wrong_entity(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    test2 = TestEntity(name="test2")
    second = SecondEntity(test_entity=test2, age=12)

    xtdb_session.put(test)
    xtdb_session.put(test2)
    xtdb_session.put(second)
    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="test").where(SecondEntity, age=12)  # No foreign key
    result = xtdb_session.query(query)

    assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": test._pk}]]

    query = query.where(SecondEntity, test_entity=TestEntity)  # Add foreign key constraint
    assert xtdb_session.query(query) == []
    assert len(xtdb_session.query(Query(TestEntity))) == 2

    xtdb_session.delete(test)
    xtdb_session.delete(test2)
    xtdb_session.delete(second)
    xtdb_session.commit()
