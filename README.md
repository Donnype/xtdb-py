![Logo](docs/logo.png)

# XTDB Python: A Python ORM for XTDB


[![Tests](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml)
[![Stable Version](https://img.shields.io/pypi/v/xtdb?label=stable)](https://pypi.org/project/xtdb/#history)
[![Python Versions](https://img.shields.io/pypi/pyversions/xtdb)](https://pypi.org/project/xtdb/)


## Installation

You can install this project using pip:

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

### Using the client

```python3
import os

from xtdb.session import XTDBClient, Operation

client = XTDBClient(os.environ["XTDB_URI"])
client.submit_transaction([Operation.put({"xt/id": "123", "name": "fred"})])

client.query('{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}')
client.get_entity("123")
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


### Using the CLI for querying

Using a query string
```bash
$ echo '{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}' | python -m xtdb | jq
[
  [
    {
      "xt/id": "123"
      "name": "fred",
      "somenil": null,
      "somedict": {
        "c": 3,
        "a": "b"
      },
    }
  ]
]
```

or a query from a file:

```bash
$ cat query.txt
{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}
$ python -m xtdb < query.txt | jq
[
  [
    {
      "xt/id": "123"
      "name": "fred",
      "somenil": null,
      "somedict": {
        "c": 3,
        "a": "b"
      },
    }
  ]
]
```

## Contributing


### Installation

To get started, clone the repo and create an environment using Poetry
```bash
$ git clone https://github.com/Donnype/xtdb-py.git
$ cd xtdb-py
$ poetry install
```

Now set up XTDB, for instance using Docker
```bash
$ docker run -p 3000:3000 -d juxt/xtdb-standalone-rocksdb:1.21.0
```

Export the XTDB_URI environment variable to be able to use `os.environ["XTDB_URI"]` to fetch the endpoint
```bash
$ export XTDB_URI=http://localhost:3000/_xtdb
```

### Development

The `Makefile` has several targets that should make development easier:
```bash
$ make utest  # Run unit tests
$ make itest  # Run integration tests
$ make check  # Run all linters
$ make done   # Run all of the above
```

The CI runs these checks as well.
Check out the [project page](https://github.com/users/Donnype/projects/1) for issues and features to work on.
