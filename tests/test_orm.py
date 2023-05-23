from tests.conftest import FirstEntity, SecondEntity


def test_proper_dict_format():
    entity = FirstEntity(name="test")

    d = entity.dict()

    assert d == {
        "xt/id": entity.id,
        "type": "FirstEntity",
        "FirstEntity/name": "test",
    }
    assert FirstEntity.from_dict(d).name == entity.name

    entity2 = SecondEntity(first_entity=entity, age=12)
    d2 = entity2.dict()

    assert d2 == {
        "xt/id": entity2.id,
        "type": "SecondEntity",
        "SecondEntity/first_entity": entity.id,
        "SecondEntity/age": 12,
    }
    assert SecondEntity.from_dict(d2).age == entity2.age
    assert SecondEntity.from_dict(d2).first_entity == entity2.first_entity.id
