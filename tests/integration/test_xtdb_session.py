import os
from datetime import datetime, timedelta, timezone

import pytest

from tests.conftest import FourthEntity, SecondEntity, TestEntity, ThirdEntity
from xtdb.exceptions import XTDBException
from xtdb.orm import Fn
from xtdb.query import Query, Var
from xtdb.session import XTDBSession

if os.environ.get("CI") != "1":
    pytest.skip("Needs XTDB container.", allow_module_level=True)


def test_status(xtdb_session: XTDBSession):
    result = xtdb_session.client.status()
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
    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity.id}

    result = xtdb_session.query(query, tx_time=datetime.now(timezone.utc) - timedelta(seconds=1))
    assert result == []

    result = xtdb_session.query(query, tx_id=-1)
    assert result == []

    result = xtdb_session.query(query, tx_id=0)
    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity.id}

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
    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity.id}

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

    assert xtdb_session.query(query)[0].dict() == {
        "TestEntity/name": "test3",
        "type": "TestEntity",
        "xt/id": third_entity.id,
    }

    assert xtdb_session.query(query, valid_time=valid_time) == []

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

    result_entity = xtdb_session.query(query, valid_time=valid_time)[0].dict()
    assert result_entity == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity.id}

    xtdb_session.evict(entity)
    xtdb_session.commit()

    result = xtdb_session.query(query)
    assert result == []

    result = xtdb_session.query(query, valid_time=valid_time)
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

    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": test.id}

    query = query.where(TestEntity, name="test")
    result = xtdb_session.query(query)
    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": test.id}

    xtdb_session.delete(test)
    xtdb_session.delete(second1)
    xtdb_session.delete(second2)
    xtdb_session.commit()


def test_deep_queries(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second = SecondEntity(test_entity=test, age=1)
    second2 = SecondEntity(test_entity=test, age=4)
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
    results = [x.dict() for x in result]

    assert {
        "SecondEntity/age": 4,
        "type": "SecondEntity",
        "xt/id": second2.id,
        "SecondEntity/test_entity": test.id,
    } in results
    assert {
        "SecondEntity/age": 1,
        "type": "SecondEntity",
        "xt/id": second.id,
        "SecondEntity/test_entity": test.id,
    } in results

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).avg(Var("age"))
    avg_result = xtdb_session.client.query(query)
    assert avg_result == [[2.5]]

    query = (
        Query(SecondEntity)
        .where(FourthEntity, third_entity=ThirdEntity, value=15.3)
        .where(ThirdEntity, second_entity=SecondEntity)
    )
    result = xtdb_session.query(query)
    assert result[0].dict() == {
        "SecondEntity/age": 4,
        "type": "SecondEntity",
        "xt/id": second2.id,
        "SecondEntity/test_entity": test.id,
    }

    xtdb_session.delete(fourth)
    xtdb_session.delete(third)
    xtdb_session.delete(second2)
    xtdb_session.delete(second)
    xtdb_session.delete(test)
    xtdb_session.commit()


def test_aggregates(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second = SecondEntity(test_entity=test, age=1)
    second2 = SecondEntity(test_entity=test, age=4)

    xtdb_session.put(test)
    xtdb_session.put(second)
    xtdb_session.put(second2)
    xtdb_session.commit()

    query = Query(SecondEntity).count(SecondEntity)

    with pytest.raises(XTDBException):
        xtdb_session.query(query)

    count_result = xtdb_session.client.query(query)
    assert count_result == [[2]]

    count_result = xtdb_session.client.query(query.count(SecondEntity))
    assert count_result == [[2, 2]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).avg(Var("age"))
    avg_result = xtdb_session.client.query(query)
    assert avg_result == [[2.5]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).sum(Var("age"))
    sum_result = xtdb_session.client.query(query)
    assert sum_result == [[5]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).min(Var("age"))
    min_result = xtdb_session.client.query(query)
    assert min_result == [[1]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).max(Var("age"))
    max_result = xtdb_session.client.query(query)
    assert max_result == [[4]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).median(Var("age"))
    median_result = xtdb_session.client.query(query)
    assert median_result == [[2.5]]

    query = Query(SecondEntity).where(SecondEntity, age=Var("age")).variance(Var("age"))
    variance_result = xtdb_session.client.query(query)
    assert variance_result == [[2.25]]  # As the average is 2.5, the variance is sqrt((1 - 2.5)^2 * (4 - 2.5)^2) = 2.25

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

    assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": test.id}

    query = query.where(SecondEntity, test_entity=TestEntity)  # Add foreign key constraint
    assert xtdb_session.query(query) == []
    assert len(xtdb_session.query(Query(TestEntity))) == 2

    xtdb_session.delete(test)
    xtdb_session.delete(test2)
    xtdb_session.delete(second)
    xtdb_session.commit()


def test_submit_and_trigger_fn(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    second = SecondEntity(test_entity=test, age=12)
    increment_age_fn = Fn(
        function="(fn [ctx eid] (let [db (xtdb.api/db ctx) entity (xtdb.api/entity db eid)] "
        "[[:xtdb.api/put (update entity :SecondEntity/age inc)]]))",
        identifier="increment_age",
    )

    xtdb_session.put(test)
    xtdb_session.put(second)
    xtdb_session.put(increment_age_fn)
    xtdb_session.commit()

    result = xtdb_session.get(second.id)
    assert result["SecondEntity/age"] == 12

    with pytest.raises(XTDBException):
        xtdb_session.get(second.id, tx_time=datetime.now(timezone.utc) - timedelta(seconds=10))

    xtdb_session.fn(increment_age_fn, second.id)
    xtdb_session.commit()

    result = xtdb_session.get(second.id)
    assert result["SecondEntity/age"] == 13

    xtdb_session.fn(increment_age_fn, second.id)
    xtdb_session.fn(increment_age_fn, second.id)
    xtdb_session.commit()

    result = xtdb_session.get(second.id)
    assert result["SecondEntity/age"] == 15

    xtdb_session.delete(test)
    xtdb_session.delete(second)
    xtdb_session.delete(increment_age_fn)
    xtdb_session.commit()


def test_get_entity_history(xtdb_session: XTDBSession):
    test = TestEntity(name="test")
    xtdb_session.put(test, datetime(1000, 10, 10))
    xtdb_session.commit()

    test.name = "new name"
    xtdb_session.put(test, datetime(1000, 10, 10))
    xtdb_session.commit()

    test.name = "new name 2"
    xtdb_session.put(test, datetime(1000, 10, 11))
    xtdb_session.commit()

    result = xtdb_session.client.get_entity_history(test.id)
    assert len(result) == 2

    assert list(result[0].keys()) == ["txTime", "txId", "validTime", "contentHash"]
    assert list(result[1].keys()) == ["txTime", "txId", "validTime", "contentHash"]

    assert result[0]["validTime"] == "1000-10-10T00:00:00Z"
    assert result[1]["validTime"] == "1000-10-11T00:00:00Z"

    assert xtdb_session.client.get_entity_history(test.id, sort_order="desc")[1] == result[0]

    assert len(xtdb_session.client.get_entity_history(test.id, start_tx_id=result[1]["txId"])) == 1
    assert len(xtdb_session.client.get_entity_history(test.id, end_tx_id=result[1]["txId"])) == 1
    assert len(xtdb_session.client.get_entity_history(test.id, start_valid_time=datetime(1000, 10, 10, 10))) == 1
    assert len(xtdb_session.client.get_entity_history(test.id, end_valid_time=datetime(1000, 10, 10, 10))) == 1

    result = xtdb_session.client.get_entity_history(test.id, with_docs=True, with_corrections=True)
    assert len(result) == 3

    assert list(result[0].keys()) == ["txTime", "txId", "validTime", "contentHash", "doc"]
    assert result[0]["doc"]["xt/id"] == test.id
    assert result[0]["doc"]["TestEntity/name"] == "test"

    assert result[1]["doc"]["TestEntity/name"] == "new name"
    assert result[2]["doc"]["TestEntity/name"] == "new name 2"

    xtdb_session.delete(test)
