from dataclasses import dataclass, field

from xtdb.orm import Base


@dataclass
class Country(Base):
    name: str


@dataclass
class City(Base):
    country: Country

    population: int
    name: str


@dataclass
class User(Base):
    city: City
    country: Country

    name: str
