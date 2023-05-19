import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union

from requests import HTTPError, Response, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from xtdb.exceptions import XTDBException
from xtdb.orm import Base, Fn
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
    valid_time: Optional[datetime] = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_list(self):
        if self.valid_time is None:
            self.valid_time = datetime.now(timezone.utc)

        if self.type is OperationType.MATCH:
            return [self.type.value, self.value["xt/id"], self.value, self.valid_time.isoformat()]

        if self.type is OperationType.FN:
            return [self.type.value, self.value["identifier"], *self.value["args"]]

        if self.type is OperationType.PUT and "xt/fn" in self.value:
            return [self.type.value, self.value]

        return [self.type.value, self.value, self.valid_time.isoformat()]

    @classmethod
    def put(cls, document: Dict, valid_time: Optional[datetime] = None) -> "Operation":
        return cls(OperationType.PUT, document, valid_time)

    @classmethod
    def delete(cls, pk: str, valid_time: Optional[datetime] = None) -> "Operation":
        return cls(OperationType.DELETE, pk, valid_time)

    @classmethod
    def match(cls, document: Dict, valid_time: Optional[datetime] = None) -> "Operation":
        return cls(OperationType.MATCH, document, valid_time)

    @classmethod
    def evict(cls, pk: str, valid_time: Optional[datetime] = None) -> "Operation":
        return cls(OperationType.EVICT, pk, valid_time)

    @classmethod
    def fn(cls, identifier: str, *args) -> "Operation":
        return cls(OperationType.FN, {"identifier": identifier, "args": args})


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
        self._session.mount("http://", HTTPAdapter(max_retries=Retry(total=6, backoff_factor=0.5)))
        self._session.mount("https://", HTTPAdapter(max_retries=Retry(total=6, backoff_factor=0.5)))
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

    def get_entity(self, eid: str, valid_time: Optional[datetime] = None) -> Dict:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        res = self._session.get(f"{self.base_url}/entity", params={"eid": eid, "valid-time": valid_time.isoformat()})
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

    def submit_transaction(self, transaction: Union[Transaction, List]) -> None:
        if isinstance(transaction, list):
            transaction = Transaction(operations=transaction)

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

    def query(self, query: Query, valid_time: Optional[datetime] = None) -> List[Base]:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        result = self._client.query(query, valid_time)
        return [query.result_type.from_dict(document[0]) for document in result]

    def get(self, eid: str, valid_time: Optional[datetime] = None) -> Dict:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        return self._client.get_entity(eid, valid_time)

    def put(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation.put(document.dict(), valid_time))

    def delete(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation.delete(document.id, valid_time))

    def match(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation.match(document.dict(), valid_time))

    def evict(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation.evict(document.id, valid_time))

    def fn(self, function: Fn, *args) -> None:
        self._transaction.add(Operation.fn(function.identifier, *args))

    def commit(self) -> None:
        if not self._transaction.operations:
            return

        try:
            self._client.submit_transaction(self._transaction)
        finally:
            self._transaction = Transaction()
