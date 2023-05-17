import logging
import uuid
from dataclasses import asdict, dataclass
from typing import List, Type

logger = logging.getLogger(__name__)

TYPE_FIELD = "type"


@dataclass
class Base:
    @property
    def _pk(self):
        if not hasattr(self, "_pk_proxy"):
            self._pk_proxy = str(uuid.uuid4())

        return self._pk_proxy

    @classmethod
    def fields(cls):
        return cls.__dataclass_fields__

    @classmethod
    def _relations(cls) -> List[str]:
        return [key for key, value in cls.fields().items() if issubclass(value.type, Base)]

    @classmethod
    def _subclasses(cls) -> List[Type["Base"]]:
        return cls.__subclasses__()

    @classmethod
    def alias(cls):
        return cls.__name__

    def dict(self):
        result = {}
        for key, value in asdict(self).items():
            if key == "_pk":
                continue

            if issubclass(self.fields().get(key).type, Base):
                result[f"{self.alias()}/{key}"] = self.__getattribute__(key)._pk
            else:
                result[f"{self.alias()}/{key}"] = value

        result["xt/id"] = self._pk
        result["type"] = self.alias()

        return result
