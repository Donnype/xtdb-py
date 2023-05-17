import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from xtdb.orm import Base
from xtdb.session import XTDBSession


@dataclass
class TestEntity(Base):
    name: str = field(default_factory=str)


@dataclass
class SecondEntity(Base):
    age: int = field(default_factory=int)
    test_entity: TestEntity = field(default_factory=TestEntity)


@dataclass
class ThirdEntity(Base):
    test_entity: TestEntity = field(default_factory=TestEntity)
    second_entity: SecondEntity = field(default_factory=SecondEntity)


@dataclass
class FourthEntity(Base):
    third_entity: ThirdEntity = field(default_factory=ThirdEntity)
    value: float = field(default_factory=float)


@pytest.fixture
def valid_time() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def xtdb_session() -> XTDBSession:
    session = XTDBSession(os.environ["XTDB_URI"])
    session._client._session.mount("http://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))

    return session
