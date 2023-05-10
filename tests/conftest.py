import os
from dataclasses import dataclass, field
from typing import Iterator

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from xtdb.orm import Base
from xtdb.session import XTDBHTTPClient, XTDBSession


@dataclass
class TestEntity(Base):
    name: str = field(default_factory=str)


@dataclass
class SecondEntity(Base):
    age: int = field(default_factory=int)
    test_entity: TestEntity = field(default_factory=TestEntity)


@pytest.fixture
def xtdb_http_client() -> XTDBHTTPClient:
    client = XTDBHTTPClient(base_url=os.environ["XTDB_URI"])
    client._session.mount("http://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))

    return client


@pytest.fixture
def xtdb_session(xtdb_http_client: XTDBHTTPClient) -> Iterator[XTDBSession]:
    yield XTDBSession(xtdb_http_client)
