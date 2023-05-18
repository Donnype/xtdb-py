import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union

from requests import HTTPError, Response, Session

from xtdb.exceptions import XTDBException
from xtdb.orm import Base
from xtdb.query import Query

logger = logging.getLogger(__name__)


@dataclass
class XTDBStatus:
    version: Optional[str]
    revision: Optional[str]
    indexVersion: Optional[int]
    consumerState: Optional[str]
    kvStore: Optional[str]
    estimateNumKeys: Optional[int]
    size: Optional[int]


class OperationType(Enum):
    PUT = "put"
    DELETE = "delete"
    MATCH = "match"
    EVICT = "evict"
    FN = "fn"


@dataclass
class Operation:
    type: OperationType
    value: Union[str, Dict[str, Any]]
    valid_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_list(self):
        if self.type is OperationType.MATCH:
            return [self.type.value, self.value["xt/id"], self.value, self.valid_time.isoformat()]

        return [self.type.value, self.value, self.valid_time.isoformat()]


@dataclass
class Transaction:
    operations: List[Operation] = field(default_factory=list)

    def add(self, operation: Operation):
        self.operations.append(operation)

    def json(self, **kwargs):
        return json.dumps({"tx-ops": [op.to_list() for op in self.operations]}, **kwargs)


class XTDBHTTPClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._session = Session()
        self._session.headers["Accept"] = "application/json"

    @staticmethod
    def _verify_response(response: Response) -> None:
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.exception("Request failed")

            raise XTDBException from e

    def status(self) -> XTDBStatus:
        res = self._session.get(f"{self.base_url}/status")
        self._verify_response(res)

        return XTDBStatus(**res.json())

    def get_entity(self, entity_id: str, valid_time: Optional[datetime] = None) -> dict:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        res = self._session.get(
            f"{self.base_url}/entity", params={"eid": entity_id, "valid-time": valid_time.isoformat()}
        )
        self._verify_response(res)
        return res.json()

    def query(self, query: Union[str, Query], valid_time: Optional[datetime] = None) -> Union[List, Dict]:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        res = self._session.post(
            f"{self.base_url}/query",
            params={"valid-time": valid_time.isoformat()},
            data=str(query),
            headers={"Content-Type": "application/edn"},
        )
        self._verify_response(res)
        return res.json()

    def await_transaction(self, transaction_id: int) -> None:
        self._session.get(f"{self.base_url}/await-tx", params={"txId": transaction_id})
        logger.info("Transaction completed [txId=%s]", transaction_id)

    def submit_transaction(self, transaction: Transaction) -> None:
        res = self._session.post(
            f"{self.base_url}/submit-tx",
            data=transaction.json(),
            headers={"Content-Type": "application/json"},
        )

        self._verify_response(res)
        self.await_transaction(res.json()["txId"])


class XTDBSession:
    def __init__(self, base_url: str):
        self._client = XTDBHTTPClient(base_url)
        self._transaction = Transaction()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Type[Exception], _exc_value: str, _exc_traceback: str) -> None:
        self.commit()

    def query(self, query: Query, valid_time: Optional[datetime] = None) -> Union[List, Dict]:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        return self._client.query(query, valid_time)

    def put(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.PUT, value=document.dict(), valid_time=valid_time))

    def delete(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.DELETE, value=document._pk, valid_time=valid_time))

    def match(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.MATCH, value=document.dict(), valid_time=valid_time))

    def evict(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.EVICT, value=document._pk, valid_time=valid_time))

    def fn(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.FN, value=document.dict(), valid_time=valid_time))

    def commit(self) -> None:
        if not self._transaction.operations:
            return

        try:
            self._client.submit_transaction(self._transaction)
        finally:
            self._transaction = Transaction()
