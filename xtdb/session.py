import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Type, Union

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


class XTDBClient:
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

    def get_entity(
        self,
        eid: str,
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
    ) -> Dict:
        params = {"eid": eid}

        if valid_time is not None:
            params["valid-time"] = valid_time.isoformat()

        if tx_time is not None:
            params["tx-time"] = tx_time.isoformat()

        if tx_id is not None:
            params["tx-id"] = str(tx_id)

        res = self._session.get(f"{self.base_url}/entity", params=params)
        self._verify_response(res)
        return res.json()

    def get_entity_transactions(
        self,
        eid: str,
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
    ) -> Dict:
        params = {"eid": eid}

        if valid_time is not None:
            params["valid-time"] = valid_time.isoformat()

        if tx_time is not None:
            params["tx-time"] = tx_time.isoformat()

        if tx_id is not None:
            params["tx-id"] = str(tx_id)

        res = self._session.get(f"{self.base_url}/entity-tx", params=params)
        self._verify_response(res)
        return res.json()

    def get_entity_history(
        self,
        eid: str,
        *,
        sort_order: Literal["asc", "desc"] = "asc",
        with_corrections: bool = False,
        with_docs: bool = False,
        start_valid_time: Optional[datetime] = None,
        start_tx_time: Optional[datetime] = None,
        start_tx_id: Optional[int] = None,
        end_valid_time: Optional[datetime] = None,
        end_tx_time: Optional[datetime] = None,
        end_tx_id: Optional[int] = None,
    ) -> Dict:
        params = {
            "eid": eid,
            "history": "true",
            "sortOrder": sort_order,
            "with-corrections": str(with_corrections).lower(),
            "with-docs": str(with_docs).lower(),
        }

        if start_valid_time is not None:
            params["start-valid-time"] = start_valid_time.isoformat()

        if start_tx_time is not None:
            params["start-tx-time"] = start_tx_time.isoformat()

        if start_tx_id is not None:
            params["start-tx-id"] = str(start_tx_id)

        if end_valid_time is not None:
            params["end-valid-time"] = end_valid_time.isoformat()

        if end_tx_time is not None:
            params["end-tx-time"] = end_tx_time.isoformat()

        if end_tx_id is not None:
            params["end-tx-id"] = str(end_tx_id)

        res = self._session.get(f"{self.base_url}/entity", params=params)
        self._verify_response(res)
        return res.json()

    def get_attribute_stats(self):
        res = self._session.get(f"{self.base_url}/attribute-stats")
        self._verify_response(res)

        return res.json()

    def sync(self, timeout: Optional[int] = None):
        params = {} if timeout is None else {"timeout": timeout}

        res = self._session.get(f"{self.base_url}/sync", params=params)
        self._verify_response(res)

        return res.json()

    def query(
        self,
        query: Union[str, Query],
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
    ) -> Union[List, Dict]:
        params = {}

        if valid_time is not None:
            params["valid-time"] = valid_time.isoformat()

        if tx_time is not None:
            params["tx-time"] = tx_time.isoformat()

        if tx_id is not None:
            params["tx-id"] = str(tx_id)

        res = self._session.post(
            f"{self.base_url}/query",
            params=params,
            data=str(query),
            headers={"Content-Type": "application/edn"},
        )
        self._verify_response(res)
        return res.json()

    def await_transaction(self, tx_id: int, timeout: Optional[int] = None) -> None:
        params = {"txId": tx_id} if timeout is None else {"txId": tx_id, "timeout": timeout}

        self._session.get(f"{self.base_url}/await-tx", params=params)

    def await_transaction_time(self, tx_time: datetime, timeout: Optional[int] = None) -> None:
        params = {"tx-time": tx_time.isoformat()}

        if timeout is not None:
            params["timeout"] = str(timeout)

        self._session.get(f"{self.base_url}/await-tx-time", params=params)

    def get_transaction_log(self, after_tx_id: Optional[int] = None, with_ops: Optional[bool] = None):
        params = {}

        if after_tx_id is not None:
            params["after-tx-id"] = str(after_tx_id)

        if with_ops is not None:
            params["with-ops?"] = str(with_ops).lower()

        res = self._session.get(f"{self.base_url}/tx-log", params=params)
        self._verify_response(res)

        return res.json()

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

    def get_transaction_committed(self, tx_id: int):
        res = self._session.get(f"{self.base_url}/tx-committed", params={"tx-id": tx_id})

        self._verify_response(res)
        return res.json()

    def get_latest_completed_transaction(self):
        res = self._session.get(f"{self.base_url}/latest-completed-tx")

        self._verify_response(res)
        return res.json()

    def get_latest_submitted_transaction(self):
        res = self._session.get(f"{self.base_url}/latest-submitted-tx")

        self._verify_response(res)
        return res.json()

    def get_active_queries(self):
        res = self._session.get(f"{self.base_url}/active-queries")

        self._verify_response(res)
        return res.json()

    def get_recent_queries(self):
        res = self._session.get(f"{self.base_url}/recent-queries")

        self._verify_response(res)
        return res.json()

    def get_slowest_queries(self):
        res = self._session.get(f"{self.base_url}/slowest-queries")

        self._verify_response(res)
        return res.json()


class XTDBSession:
    def __init__(self, base_url: str):
        self.client = XTDBClient(base_url)
        self._transaction = Transaction()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Type[Exception], _exc_value: str, _exc_traceback: str) -> None:
        self.commit()

    def query(self, query: Query, **kwargs) -> List[Base]:
        if not query._preserved_return_type:
            raise XTDBException(
                "XTDBSession.query() only supports queries with preserved return types. Use XTDBClient.query() instead."
            )
        result = self.client.query(query, **kwargs)

        return [query.result_type.from_dict(document[0]) for document in result]

    def get(self, eid: str, **kwargs) -> Dict:
        return self.client.get_entity(eid, **kwargs)

    def put(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        self._transaction.add(Operation.put(document.dict(), valid_time))

    def delete(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        self._transaction.add(Operation.delete(document.id, valid_time))

    def match(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        self._transaction.add(Operation.match(document.dict(), valid_time))

    def evict(self, document: Base, valid_time: Optional[datetime] = None) -> None:
        self._transaction.add(Operation.evict(document.id, valid_time))

    def fn(self, function: Fn, *args) -> None:
        self._transaction.add(Operation.fn(function.identifier, *args))

    def commit(self) -> None:
        if not self._transaction.operations:
            return

        try:
            self.client.submit_transaction(self._transaction)
        finally:
            self._transaction = Transaction()
