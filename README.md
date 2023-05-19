# XTDB Python

A Python connector and ORM for XTDB.

[![Code Integration](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml)
[![Pypi release](https://github.com/Donnype/xtdb-py/actions/workflows/release.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/release.yml)

## Installation

```bash
pip install xtdb
```

## Usage

The following examples assume you have set the `XTDB_URI` variable in your environment.
To start experimenting, you could use the following setup using Docker:
```bash
$ docker run -p 3000:3000 -d juxt/xtdb-standalone-rocksdb:1.21.0
$ export XTDB_URI=http://localhost:3000/_xtdb
```

### Using the ORM

```python3
import os
from dataclasses import dataclass

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

assert result[0].dict() == {"TestEntity/name": "test", "type": "TestEntity", "xt/id": entity.id}
```

### Using only the client

```python3
import os

from xtdb.session import XTDBHTTPClient, Operation

client = XTDBHTTPClient(os.environ["XTDB_URI"])
client.submit_transaction([Operation.put({"xt/id": "123", "name": "fred"})])

client.query('{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}')
client.get_entity("123")
```

### Using the CLI tool for querying

Using a query string
```bash
$ echo '{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}' | python -m xtdb | jq
[
  [
    {
      "somenil": null,
      "name": "fred",
      "somedict": {
        "c": 3,
        "a": "b"
      },
      "somelist": [
        "jeez",
        123
      ],
      "xt/id": "123"
    }
  ]
]
```

or a query in a file:

```bash
$ cat query.txt
{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}
$ python -m xtdb < query.txt | jq
[
  [
    {
      "somenil": null,
      "name": "fred",
      "somedict": {
        "c": 3,
        "a": "b"
      },
      "somelist": [
        "jeez",
        123
      ],
      "xt/id": "123"
    }
  ]
]
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
