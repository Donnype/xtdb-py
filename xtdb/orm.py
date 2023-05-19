import logging
import uuid
from dataclasses import asdict, dataclass
from typing import Dict, List, Type

logger = logging.getLogger(__name__)

TYPE_FIELD = "type"


@dataclass
class Base:
    @property
    def id(self):
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

    def dict(self) -> Dict:
        result = {}

        for key, value in asdict(self).items():
            if issubclass(self.fields().get(key).type, Base):
                field = self.__getattribute__(key)

                # Foreign keys are not always hydrated
                result[f"{self.alias()}/{key}"] = field.id if isinstance(field, Base) else field
            else:
                result[f"{self.alias()}/{key}"] = value

        result["xt/id"] = self.id
        result["type"] = self.alias()

        return result

    @classmethod
    def from_dict(cls, document: Dict) -> "Base":
        doc = {key.replace(f"{cls.alias()}/", ""): value for key, value in document.items()}
        pk = doc["xt/id"]

        del doc["xt/id"]
        del doc["type"]

        instance = cls(**doc)
        instance._pk_proxy = pk

        return instance


@dataclass
class Fn(Base):
    function: str
    identifier: str

    @property
    def id(self):
        return self.identifier

    def dict(self) -> Dict:
        return {
            "xt/id": self.identifier,
            "xt/fn": self.function,
        }
