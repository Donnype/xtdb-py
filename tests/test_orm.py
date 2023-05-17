from tests.conftest import SecondEntity, TestEntity


def test_proper_dict_format():
    entity = TestEntity(name="test")

    assert entity.dict() == {
        "xt/id": entity._pk,
        "type": "TestEntity",
        "TestEntity/name": "test",
    }

    entity2 = SecondEntity(test_entity=entity, age=12)
    assert entity2.dict() == {
        "xt/id": entity2._pk,
        "type": "SecondEntity",
        "SecondEntity/test_entity": entity._pk,
        "SecondEntity/age": 12,
    }
