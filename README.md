![Logo](https://raw.githubusercontent.com/Donnype/xtdb-py/main/docs/source/_static/logo.png)

# XTDB Python: A Python ORM for [XTDB](https://www.xtdb.com/)

<div align="center">

[![Python Versions](https://img.shields.io/pypi/pyversions/xtdb)](https://pypi.org/project/xtdb/)
[![Stable Version](https://img.shields.io/pypi/v/xtdb?label=stable)](https://pypi.org/project/xtdb/#history)

[![Tests](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml/badge.svg)](https://github.com/Donnype/xtdb-py/actions/workflows/tests.yml)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/Donnype/xtdb-py/blob/main/.pre-commit-config.yaml)
[![License](https://img.shields.io/github/license/Donnype/xtdb-py)](https://github.com/Donnype/xtdb-py/blob/main/LICENSE)

</div>

Check out [the documentation](https://donnype.github.io/xtdb-py/) for a more complete documentation of the package.


## Installation

You can install this project using pip:

```bash
$ pip install xtdb
```


## Usage

The following examples assume you have set the `XTDB_URI` variable in your environment.
To start experimenting, you could use the following setup using Docker:
```bash
$ docker run -p 3000:3000 -d juxt/xtdb-standalone-rocksdb:1.21.0
$ export XTDB_URI=http://localhost:3000/_xtdb
```

### Using the Client

The `XTDBClient` supports the full [HTTP API spec](https://docs.xtdb.com/clients/http/).

```python3
>> > import os
>> > from xtdb.session import XTDBClient, Operation
>> >
>> > client = XTDBClient(os.environ["XTDB_URI"])
>> > client.submit_tx([Operation.put({"xt/id": "123", "name": "fred"})])
>> >
>> > client.query('{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}')
[[{'name': 'fred', 'xt/id': '123'}]]
>> >
>> > client.get_entity("123")
{'name': 'fred', 'xt/id': '123'}
```

Take a look at the spec to see the full range of functionality that maps directly to the client.

### Using the Datalog module

The [datalog](https://github.com/Donnype/xtdb-py/blob/main/xtdb/datalog.py) module also provides a layer to construct queries with more easily.
Given the data from [the cities example](https://github.com/Donnype/xtdb-py/blob/main/examples/cities) has been seeded:
```python3
>>> from xtdb.datalog import Find, Where
>>>
>>> query = Find("(pull Country [*])") & Find("City") & (Where("City", "City/country", "Country") & Where("City", "City/name", '"Rome"'))
>>> str(query)
{:query {:find [ (pull Country [*]) City] :where [ [ City :City/country Country ] [ City :City/name "Rome" ]]}}
>>>
>>> client.query(query)
[[{'type': 'Country', 'Country/name': 'Italy', 'xt/id': 'c095839f-031f-46ad-85e1-097f634ba4f0'}, '33aa7fa6-b752-4982-a772-d2dbaeda58ae']]
```

To see more datalog query examples, check out the [unit tests](https://github.com/Donnype/xtdb-py/blob/main/tests/test_datalog.py).

### Using the ORM and Session

Below is an example of how to use the ORM functionality.

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

with session:
    session.put(entity)

query = Query(TestEntity).where(TestEntity, name="test")
result = session.query(query)

result[0].dict() #  {"TestEntity/name": "test", "type": "TestEntity", "xt/id": "fe2a3ee0-9254-41dc-91cc-74ad9e2a16db"}
```

To see more examples, check out the [examples directory](https://github.com/Donnype/xtdb-py/blob/main/examples).
Don't hesitate to add your own examples!

### Using the CLI for querying

This package also comes with an easy CLI tool to query XTDB.
Again, set the `XTDB_URI` variable in your environment first and you will be able to query XTDB as follows:

```bash
$ echo '{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}' | python -m xtdb
[[{"name": "fred", "xt/id": "123"}]]
```

To use a query from a file, run:

```bash
$ cat query.txt
{:query {:find [(pull ?e [*])] :where [[ ?e :name "fred" ]]}}
$
$ python -m xtdb < query.txt
[[{"name": "fred", "xt/id": "123"}]]
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
