import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from xtdb.orm import Base
from xtdb.session import XTDBSession


@dataclass
class TestEntity(Base):
    name: str


@dataclass
class SecondEntity(Base):
    age: int
    test_entity: TestEntity


@dataclass
class ThirdEntity(Base):
    test_entity: TestEntity
    second_entity: SecondEntity


@dataclass
class FourthEntity(Base):
    third_entity: ThirdEntity
    value: float


@pytest.fixture
def valid_time() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def xtdb_session() -> XTDBSession:
    session = XTDBSession(os.environ["XTDB_URI"])
    session.client._session.mount("http://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))

    return session
