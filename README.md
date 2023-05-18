# XTDB Python

A Python connector and ORM for XTDB.

[![Code Integration](https://github.com/Donnype/xtdb-py/actions/workflows/code-integration.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/code-integration.yml)

## Installation

```bash
pip install xtdb
```

## Usage


### Using the ORM

```python3
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

### Using only the client

```python3
import os

from xtdb.session import XTDBHTTPClient, Transaction, Operation

client = XTDBHTTPClient(os.environ["XTDB_URI"])
client.submit_transaction(Transaction([Operation.put({"xt/id": "123", "name": "fred"})]))

client.query('{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}')
client.get_entity("123")
```

## Development

Using Poetry, simply run

```bash
poetry install
```

to create your environment. The `Makefile` has several targets that should make development easier:
```bash
$ make utest  # Run unit tests
$ make itest  # Run integration tests
$ make check  # Run all linters
$ make done   # Run all of the above
```
