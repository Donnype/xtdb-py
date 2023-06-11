"""
A module containing the logic to generate XTDB queries using the ORM models.
"""

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple, Type, Union

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
    OrderBy,
    Rand,
    Sample,
    Stddev,
    Sum,
    Timeout,
    Variance,
    Where,
)
from xtdb.exceptions import InvalidField
from xtdb.orm import TYPE_FIELD, Base


@dataclass
class Var:
    val: str

    def __str__(self):
        return f"?{self.val}"


@dataclass
class Query:
    """
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
    _limit: Optional[Limit] = None
    _order_by: Optional[OrderBy] = None
    _offset: Optional[Offset] = None
    _timeout: Optional[Timeout] = None

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

    def order_by(self, fields: List[Tuple[str, Literal["asc", "desc"]]]) -> "Query":
        self._order_by = OrderBy(fields)

        return self

    def limit(self, limit: int) -> "Query":
        self._limit = Limit(limit)

        return self

    def offset(self, offset: int) -> "Query":
        self._offset = Offset(offset)

        return self

    def timeout(self, timeout: int) -> "Query":
        self._timeout = Timeout(timeout)

        return self

    def _where_field_is(
        self, object_type: Type[Base], field_name: str, value: Union[Type[Base], Var, str, None]
    ) -> None:
        if field_name not in object_type.fields():
            raise InvalidField(f'"{field_name}" is not a field of {object_type.alias()}')

        if isinstance(value, str):
            value = value.replace('"', r"\"")
            return self._add_where_statement(object_type, field_name, f'"{value}"')

        if isinstance(value, (int, float, bool, Var)):
            return self._add_where_statement(object_type, field_name, f"{str(value).lower()}")

        if value is None:
            return self._add_where_statement(object_type, field_name, "nil")

        # TODO: support for list and dict?
        if not isinstance(value, type):
            raise InvalidField(f"value '{value}' should be a string or a Base Type")
        if not issubclass(value, Base):
            raise InvalidField(f"{value} is not an Base Type")
        if field_name not in object_type.relations():
            raise InvalidField(f'"{field_name}" is not a relation of {object_type.alias()}')

        if object_type.subclasses():
            return self._add_or_statement(object_type, field_name, value.alias())

        self._add_where_statement(object_type, field_name, value.alias())

    def _add_where_statement(self, object_type: Type[Base], field_name: str, to_alias: str) -> None:
        self._where = self._where & Where(object_type.alias(), f"{object_type.alias()}/{field_name}", to_alias)

    def _add_or_statement(self, object_type: Type[Base], field_name: str, to_alias: str) -> None:
        clauses = [
            Where(object_type.alias(), f"{sc.alias()}/{field_name}", to_alias) for sc in object_type.subclasses()
        ]
        self._where = self._where & Or(clauses)  # type: ignore

    def _compile(self, *, separator=" ") -> str:
        where = self._where & Where(self.result_type.alias(), TYPE_FIELD, f'"{self.result_type.alias()}"')
        find = Find(f"(pull {self.result_type.alias()} [*])") if self._find is None else self._find
        find_where = find & where & self._order_by & self._limit & self._offset & self._timeout

        return find_where.compile(separator=separator)

    def __str__(self) -> str:
        return self._compile()

    def __eq__(self, other):
        return str(self) == str(other)
