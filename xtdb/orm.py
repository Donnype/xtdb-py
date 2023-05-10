import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from http import HTTPStatus
from typing import List, Optional, Dict, Union, Any, Type

from requests import Session, HTTPError, Response

logger = logging.getLogger(__name__)

TYPE_FIELD = "type"


@dataclass
class Base:
    _pk: str = field(default_factory=lambda: str(uuid.uuid4()))
    _concrete: bool = True

    @classmethod
    def fields(cls):
        return cls.__dataclass_fields__

    @classmethod
    def _relations(cls) -> List[str]:
        return [key for key, value in cls.fields().items() if issubclass(value.type, Base)]

    @classmethod
    def alias(cls):
        return cls.__name__

    def dict(self):
        result = {}
        for key, value in asdict(self).items():
            if key in ["_concrete", "_pk"]:
                continue

            if issubclass(self.fields().get(key).type, Base):
                result[f"{self.alias()}/{key}"] = value["_pk"]
            else:
                result[f"{self.alias()}/{key}"] = value

        result["xt/id"] = self._pk
        result["type"] = self.alias()

        return result


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


@dataclass
class Transaction:
    operations: List[Operation] = field(default_factory=list)

    def add(self, operation: Operation):
        self.operations.append(operation)

    def json(self):
        return json.dumps({"tx-ops": [[op.type.value, op.value, op.valid_time.isoformat()] for op in self.operations]})


@dataclass
class XTDBStatus:
    version: Optional[str]
    revision: Optional[str]
    indexVersion: Optional[int]
    consumerState: Optional[str]
    kvStore: Optional[str]
    estimateNumKeys: Optional[int]
    size: Optional[int]


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
            if e.response.status_code != HTTPStatus.NOT_FOUND:
                logger.exception(response.request.url)
            raise e

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

    def query(self, query: str, valid_time: Optional[datetime] = None) -> Union[List, Dict]:
        if valid_time is None:
            valid_time = datetime.now(timezone.utc)

        res = self._session.post(f"{self.base_url}/query",params={"valid-time": valid_time.isoformat()},data=query,headers={"Content-Type": "application/edn"},)
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
    def __init__(self, client: XTDBHTTPClient):
        self.client = client
        self._transaction = Transaction()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Type[Exception], _exc_value: str, _exc_traceback: str) -> None:
        self.commit()

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

        self._transaction.add(Operation(type=OperationType.EVICT, value=document.dict(), valid_time=valid_time))

    def fn(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        if not valid_time:
            valid_time = datetime.now(timezone.utc)

        self._transaction.add(Operation(type=OperationType.FN, value=document.dict(), valid_time=valid_time))

    def commit(self) -> None:
        if self._transaction:
            logger.debug(self._transaction)
            self.client.submit_transaction(self._transaction)

        self._transaction = Transaction()
