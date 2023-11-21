"""
A module containing all logic related to connecting to an XTDB node and managing transactional scope.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from json import JSONDecodeError
from typing import Any, Dict, List, Literal, Optional, Type, Union

from requests import HTTPError, Response, Session
from requests.adapters import DEFAULT_POOLBLOCK, DEFAULT_POOLSIZE, HTTPAdapter
from requests.exceptions import ConnectionError
from urllib3 import Retry

from xtdb.datalog import Clause, FindWhere
from xtdb.exceptions import XTDBException
from xtdb.orm import Base, Fn
from xtdb.query import Query

logger = logging.getLogger("XTDB")


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
    def __init__(
        self,
        base_url: str,
        pool_connections: int = DEFAULT_POOLSIZE,
        pool_maxsize: int = DEFAULT_POOLSIZE,
        pool_block: bool = DEFAULT_POOLBLOCK,
        retries: int = 6,
        backoff_factor: float = 0.5,
    ):
        self.base_url = base_url
        self.adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            max_retries=Retry(total=retries, backoff_factor=backoff_factor, connect=3),
        )
        self._session = self.get_session()

    def get_session(self) -> Session:
        session = Session()

        session.mount("http://", self.adapter)
        session.mount("https://", self.adapter)
        session.headers["Accept"] = "application/json"
        session.hooks["response"] = self._verify_response

        logger.debug("Initialized new HTTP session")

        return session

    def refresh(self):
        self._session = self.get_session()

    @staticmethod
    def _verify_response(response: Response, *args, **kwargs) -> None:
        logger.debug('"%s %s" %s', response.request.method, response.request.url, response.status_code)

        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.exception("Request failed")

            raise XTDBException(str(e)) from e

    def status(self) -> XTDBStatus:
        return XTDBStatus(**self._session.get(f"{self.base_url}/status").json())

    def get_entity(
        self,
        eid: str,
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
    ) -> Dict:
        params = self._format_parameter("eid", eid)
        params = self._format_parameter("valid-time", valid_time, params)
        params = self._format_parameter("tx-time", tx_time, params)
        params = self._format_parameter("tx-id", tx_id, params)

        res = self._session.get(f"{self.base_url}/entity", params=params)
        return res.json()

    def get_entity_transactions(
        self,
        eid: str,
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
    ) -> Dict:
        params = self._format_parameter("eid", eid)
        params = self._format_parameter("valid-time", valid_time, params)
        params = self._format_parameter("tx-time", tx_time, params)
        params = self._format_parameter("tx-id", tx_id, params)

        return self._session.get(f"{self.base_url}/entity-tx", params=params).json()

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
        params = self._format_parameter("eid", eid)
        params = self._format_parameter("history", True, params)
        params = self._format_parameter("sort-order", sort_order, params)
        params = self._format_parameter("with-corrections", with_corrections, params)
        params = self._format_parameter("with-docs", with_docs, params)
        params = self._format_parameter("start-valid-time", start_valid_time, params)
        params = self._format_parameter("start-tx-time", start_tx_time, params)
        params = self._format_parameter("start-tx-id", start_tx_id, params)
        params = self._format_parameter("end-valid-time", end_valid_time, params)
        params = self._format_parameter("end-tx-time", end_tx_time, params)
        params = self._format_parameter("end-tx-id", end_tx_id, params)

        return self._session.get(f"{self.base_url}/entity", params=params).json()

    def get_attribute_stats(self):
        return self._session.get(f"{self.base_url}/attribute-stats").json()

    def sync(self, timeout: Optional[int] = None):
        return self._session.get(f"{self.base_url}/sync", params=self._format_parameter("timeout", timeout)).json()

    def query(
        self,
        query: Union[str, Query, Clause],
        *,
        valid_time: Optional[datetime] = None,
        tx_time: Optional[datetime] = None,
        tx_id: Optional[int] = None,
        tries: int = 0,
    ) -> Union[List, Dict]:
        if not isinstance(query, (str, Query)) and not issubclass(type(query), FindWhere):
            raise XTDBException("Cannot query using incomplete clause")

        params = self._format_parameter("valid-time", valid_time)
        params = self._format_parameter("tx-time", tx_time, params)
        params = self._format_parameter("tx-id", tx_id, params)

        try:
            return self._session.post(
                f"{self.base_url}/query", str(query), params=params, headers={"Content-Type": "application/edn"}
            ).json()
        except JSONDecodeError as e:
            if e.msg == "Expecting value":
                # Empty bodies are returned when you do strange queries such as Sum(x) where x is not numerical.
                raise XTDBException("Bad XTDB response: query probably failed") from e
            raise
        except ConnectionError:
            if tries > 0:
                raise

            # Bad queries cleave connections in a bad state, which is fixed by creating a new requests.Session()
            self.refresh()
            return self.query(query, valid_time=valid_time, tx_time=tx_time, tx_id=tx_id, tries=1)

    def await_transaction(self, tx_id: int, timeout: Optional[int] = None) -> None:
        params = self._format_parameter("timeout", timeout)
        params = self._format_parameter("tx-id", tx_id, params)

        self._session.get(f"{self.base_url}/await-tx", params=params)

    def await_transaction_time(self, tx_time: datetime, timeout: Optional[int] = None) -> None:
        params = self._format_parameter("tx-time", tx_time)
        params = self._format_parameter("timeout", timeout, params)

        self._session.get(f"{self.base_url}/await-tx-time", params=params)

    def get_transaction_log(self, after_tx_id: Optional[int] = None, with_ops: Optional[bool] = None):
        params = self._format_parameter("after-tx-id", after_tx_id)
        params = self._format_parameter("with-ops?", with_ops, params)

        return self._session.get(f"{self.base_url}/tx-log", params=params).json()

    def submit_tx(self, transaction: Union[Transaction, List], tries: int = 0) -> None:
        if isinstance(transaction, list):
            transaction = Transaction(operations=transaction)

        try:
            res = self._session.post(
                f"{self.base_url}/submit-tx", transaction.json(), headers={"Content-Type": "application/json"}
            )
        except ConnectionError:
            if tries > 0:
                raise

            # Bad queries cleave connections in a bad state, which is fixed by creating a new requests.Session()
            self.refresh()
            return self.submit_tx(transaction, tries=1)

        self.await_transaction(res.json()["txId"])

    def get_transaction_committed(self, tx_id: int):
        return self._session.get(f"{self.base_url}/tx-committed", params=self._format_parameter("tx-id", tx_id)).json()

    def get_latest_completed_transaction(self):
        return self._session.get(f"{self.base_url}/latest-completed-tx").json()

    def get_latest_submitted_transaction(self):
        return self._session.get(f"{self.base_url}/latest-submitted-tx").json()

    def get_active_queries(self):
        return self._session.get(f"{self.base_url}/active-queries").json()

    def get_recent_queries(self):
        return self._session.get(f"{self.base_url}/recent-queries").json()

    def get_slowest_queries(self):
        return self._session.get(f"{self.base_url}/slowest-queries").json()

    @staticmethod
    def _format_parameter(
        key: str, parameter: Union[None, datetime, int, str, bool], current_params: Optional[Dict] = None
    ) -> Dict:
        if current_params is None:
            current_params = {}

        if isinstance(parameter, datetime):
            current_params[key] = parameter.isoformat()
        if isinstance(parameter, (int, str, bool)):
            current_params[key] = str(parameter).lower()

        return current_params


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
        if operation_count := len(self._transaction.operations) == 0:
            return

        try:
            self.client.submit_tx(self._transaction)
            logger.debug("Committed %s operations", operation_count)
        finally:
            self._transaction = Transaction()
