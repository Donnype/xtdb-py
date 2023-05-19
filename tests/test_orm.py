from tests.conftest import SecondEntity, TestEntity


def test_proper_dict_format():
    entity = TestEntity(name="test")

    d = entity.dict()

    assert d == {
        "xt/id": entity.id,
        "type": "TestEntity",
        "TestEntity/name": "test",
    }
    assert TestEntity.from_dict(d).name == entity.name

    entity2 = SecondEntity(test_entity=entity, age=12)
    d2 = entity2.dict()

    assert d2 == {
        "xt/id": entity2.id,
        "type": "SecondEntity",
        "SecondEntity/test_entity": entity.id,
        "SecondEntity/age": 12,
    }
    assert SecondEntity.from_dict(d2).age == entity2.age
    assert SecondEntity.from_dict(d2).test_entity == entity2.test_entity.id
