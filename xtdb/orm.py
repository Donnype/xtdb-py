import logging
import uuid
from dataclasses import asdict, dataclass, field
from typing import List

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
