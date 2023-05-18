# XTDB Python

A Python connector and ORM for XTDB.

[![Code Integration](https://github.com/Donnype/xtdb-py/actions/workflows/code-integration.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/code-integration.yml)

## Installation

```bash
pip install xtdb
```

## Usage


```python
import os
from dataclasses import dataclass, field

from xtdb.orm import Base
from xtdb.query import Query
from xtdb.session import XTDBSession


@dataclass
class TestEntity(Base):
    name: str


@dataclass
class SecondEntity(Base):
    age: int
    test_entity: TestEntity


session = XTDBSession(os.environ["XTDB_URI"])

entity = TestEntity(name="test")
session.put(entity)
session.commit()

query = Query(TestEntity).where(TestEntity, name="test")
result = session.query(query)

assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity._pk}
```

## Development

Using Poetry, simply run:

```bash
poetry install
```
