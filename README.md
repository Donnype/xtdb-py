# XTDB Python

An ORM for XTDB.



## Installation

```bash
poetry install
```

## Usage


```python
import os
from dataclasses import dataclass, field

from xtdb.orm import Base
from xtdb.query import Query
from xtdb.session import XTDBHTTPClient, XTDBSession


@dataclass
class TestEntity(Base):
    name: str = field(default_factory=str)


@dataclass
class SecondEntity(Base):
    age: int = field(default_factory=int)
    test_entity: TestEntity = field(default_factory=TestEntity)


client = XTDBHTTPClient(os.environ["XTDB_URI"])
session = XTDBSession(client)

entity = TestEntity(name="test")
xtdb_session.put(entity)
xtdb_session.commit()

query = Query(TestEntity).where(TestEntity, name="test")
result = xtdb_session.client.query(query)

assert result == [[{"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity._pk}]]
```
