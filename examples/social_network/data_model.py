from dataclasses import dataclass, field

from xtdb.orm import Base


@dataclass
class Country(Base):
    name: str = field(default_factory=str)


@dataclass
class City(Base):
    country: Country = field(default_factory=Country)

    population: int = field(default_factory=int)
    name: str = field(default_factory=str)


@dataclass
class User(Base):
    city: City = field(default_factory=City)
    country: Country = field(default_factory=Country)

    name: str = field(default_factory=str)

