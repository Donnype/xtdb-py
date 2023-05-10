import os

import pytest

from tests.conftest import TestEntity, SecondEntity
from xtdb.orm import XTDBSession
from xtdb.query import Query

if os.environ.get("CI") != "1":
    pytest.skip("Needs XTDB container.", allow_module_level=True)


def test_status(xtdb_session: XTDBSession):
    result = xtdb_session.client.status()
    assert result.version == "1.21.0"
    assert result.kvStore == "xtdb.rocksdb.RocksKv"


def test_query_no_results(xtdb_session: XTDBSession):
    query = Query(TestEntity).where(TestEntity, name="test")

    result = xtdb_session.client.query(str(query))
    assert result == []


def test_query_simple_filter(xtdb_session: XTDBSession):
    entity = TestEntity(name="test")
    xtdb_session.put(entity)

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.client.query(str(query))
    assert result == []

    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="wrong")
    result = xtdb_session.client.query(str(query))
    assert result == []

    query = Query(TestEntity).where(TestEntity, name="test")
    result = xtdb_session.client.query(str(query))
    assert result == [
        [
            {
                "TestEntity/name": "test",
                "type": "TestEntity",
                "xt/id": entity._pk,
            }
        ]
    ]

    xtdb_session.delete(entity)
    xtdb_session.commit()


def test_query_not_empty_on_reference_filter_for_hostname(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second1 = SecondEntity(test_entity=test, age=1)
    second2 = SecondEntity(test_entity=test, age=2)

    xtdb_session.put(test)
    xtdb_session.put(second1)
    xtdb_session.put(second2)
    xtdb_session.commit()

    query = Query(TestEntity).where(SecondEntity, age=1).where(SecondEntity, test_entity=TestEntity)
    result = xtdb_session.client.query(str(query))

    assert result == [
        [
            {
                "TestEntity/name": "test",
                "type": "TestEntity",
                "xt/id": test._pk,
            }
        ]
    ]

    query = query.where(TestEntity, name="test")
    result = xtdb_session.client.query(str(query))
    assert result == [
        [
            {
                "TestEntity/name": "test",
                "type": "TestEntity",
                "xt/id": test._pk,
            }
        ]
    ]

    xtdb_session.delete(test)
    xtdb_session.delete(second1)
    xtdb_session.delete(second2)
    xtdb_session.commit()


def test_query_empty_on_reference_filter_for_wrong_hostname(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    test2 = TestEntity(name="test2")
    second = SecondEntity(test_entity=test2, age=12)

    xtdb_session.put(test)
    xtdb_session.put(test2)
    xtdb_session.put(second)
    xtdb_session.commit()

    query = Query(TestEntity).where(TestEntity, name="test").where(SecondEntity, age=12)  # No foreign key
    result = xtdb_session.client.query(str(query))

    assert result == [
        [
            {
                "TestEntity/name": "test",
                "type": "TestEntity",
                "xt/id": test._pk,
            }
        ]
    ]

    query = query.where(SecondEntity, test_entity=TestEntity)  # Add foreign key constraint
    assert xtdb_session.client.query(str(query)) == []
    assert len(xtdb_session.client.query(str(Query(TestEntity)))) == 2

    xtdb_session.delete(test)
    xtdb_session.delete(test2)
    xtdb_session.delete(second)
    xtdb_session.commit()
