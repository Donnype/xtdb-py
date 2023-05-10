import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

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
