from dataclasses import dataclass
from typing import Optional, Type, Union

from xtdb.datalog import (
    Avg,
    Clause,
    Count,
    CountDistinct,
    Distinct,
    Find,
    Limit,
    Max,
    Median,
    Min,
    Offset,
    Or,
    Rand,
    Sample,
    Stddev,
    Sum,
    Variance,
    Where,
)
from xtdb.exceptions import InvalidField
from xtdb.orm import Base


@dataclass
class Var:
    val: str

    def __str__(self):
        return f"?{self.val}"


@dataclass
class Query:
    """Object representing an XTDB query.

        result_type: Object being queried: executing the query should yield only this Object.

    Example usage:

    >>> query = Query(Object1).where(Object1, name="test")
    >>> query = query.where(Object2, object_one_reference=Object1)
    >>> query.format()
    '
    {:query {:find [(pull Object1 [*])] :where [
        [ Object1 :Object1/name "test" ]
        [ Object2 :Object2/object_one_reference Object1 ]
    ]}}
    '
    """

    result_type: Type[Base]

    _find: Optional[Clause] = None
    _where: Optional[Clause] = None

    _limit: Optional[int] = None
    _offset: Optional[int] = None
    _preserved_return_type: bool = True

    def where(self, object_type: Type[Base], **kwargs) -> "Query":
        for field_name, value in kwargs.items():
            self._where_field_is(object_type, field_name, value)

        return self

    def format(self) -> str:
        return self._compile(separator="\n    ")

    def count(self, var: Union[Type[Base], Var]) -> "Query":
        self._preserved_return_type = False

        if isinstance(var, Var):
            self._find = self._find & Count(str(var))
            return self

        self._find = self._find & Count(var.alias())
        return self

    def avg(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Avg(str(var))

        return self

    def max(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Max(str(var))

        return self

    def min(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Min(str(var))

        return self

    def count_distinct(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & CountDistinct(str(var))

        return self

    def sum(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Sum(str(var))

        return self

    def median(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Median(str(var))

        return self

    def variance(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Variance(str(var))

        return self

    def stddev(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Stddev(str(var))

        return self

    def distinct(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Distinct(str(var))

        return self

    def rand(self, var: Var, N: int) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Rand(str(var), N)

        return self

    def sample(self, var: Var, N: int) -> "Query":
        self._preserved_return_type = False
        self._find = self._find & Sample(str(var), N)

        return self

    def limit(self, limit: int) -> "Query":
        self._limit = limit

        return self

    def offset(self, offset: int) -> "Query":
        self._offset = offset

        return self

    def _where_field_is(
        self, object_type: Type[Base], field_name: str, value: Union[Type[Base], Var, str, None]
    ) -> None:
        if field_name not in object_type.fields():
            raise InvalidField(f'"{field_name}" is not a field of {object_type.alias()}')

        if isinstance(value, str):
            value = value.replace('"', r"\"")
            self._add_where_statement(object_type, field_name, f'"{value}"')
            return

        if type(value) in [int, float, bool, Var]:
            self._add_where_statement(object_type, field_name, f"{value}")
            return

        if value is None:
            self._add_where_statement(object_type, field_name, "nil")
            return

        # TODO: support for list and dict?

        if not isinstance(value, type):
            raise InvalidField(f"value '{value}' should be a string or a Base Type")

        if not issubclass(value, Base):
            raise InvalidField(f"{value} is not an Base Type")

        if field_name not in object_type._relations():
            raise InvalidField(f'"{field_name}" is not a relation of {object_type.alias()}')

        if object_type._subclasses():
            self._add_or_statement(object_type, field_name, value.alias())
            return

        self._add_where_statement(object_type, field_name, value.alias())

    def _add_where_statement(self, object_type: Type[Base], field_name: str, to_alias: str) -> None:
        self._where = self._where & Where(object_type.alias(), f"{object_type.alias()}/{field_name}", to_alias)

    def _add_or_statement(self, object_type: Type[Base], field_name: str, to_alias: str) -> None:
        clauses = [
            Where(object_type.alias(), f"{sc.alias()}/{field_name}", to_alias) for sc in object_type._subclasses()
        ]
        self._where = self._where & Or(clauses)  # type: ignore

    def _compile(self, *, separator=" ") -> str:
        where = self._where & Where(self.result_type.alias(), "type", f'"{self.result_type.alias()}"')
        find = Find(f"(pull {self.result_type.alias()} [*])") if self._find is None else self._find

        find_where = find & where

        if self._limit is not None:
            find_where = find_where & Limit(self._limit)

        if self._offset is not None:
            find_where = find_where & Offset(self._offset)

        return find_where.compile(separator=separator)

    def __str__(self) -> str:
        return self._compile()

    def __eq__(self, other):
        return str(self) == str(other)
