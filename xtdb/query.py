from dataclasses import dataclass, field
from typing import List, Optional, Type, Union

from xtdb.exceptions import InvalidField
from xtdb.orm import TYPE_FIELD, Base


@dataclass
class Var:
    val: str


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

    _where_clauses: List[str] = field(default_factory=list)
    _find_clauses: List[str] = field(default_factory=list)
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
            self._find_clauses.append(f"(count ?{var.val})")
            return self

        self._find_clauses.append(f"(count {var.alias()})")
        return self

    def avg(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(avg ?{var.val})")

        return self

    def max(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(max ?{var.val})")

        return self

    def min(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(min ?{var.val})")

        return self

    def count_distinct(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(count-distinct ?{var.val})")

        return self

    def sum(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(sum ?{var.val})")

        return self

    def median(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(median ?{var.val})")

        return self

    def variance(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(variance ?{var.val})")

        return self

    def stddev(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(stddev ?{var.val})")

        return self

    def distinct(self, var: Var) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(distinct ?{var.val})")

        return self

    def rand(self, var: Var, N: int) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(rand {N} ?{var.val})")

        return self

    def sample(self, var: Var, N: int) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"(sample {N} ?{var.val})")

        return self

    def group_by(self, object_type: Type[Base]) -> "Query":
        self._preserved_return_type = False
        self._find_clauses.append(f"{object_type.alias()}")

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

        if isinstance(value, Var):
            self._add_where_statement(object_type, field_name, f"?{value.val}")
            return

        if isinstance(value, str):
            value = value.replace('"', r"\"")
            self._add_where_statement(object_type, field_name, f'"{value}"')
            return

        if type(value) in [int, float, bool]:
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
        self._where_clauses.append(self._relationship(object_type.alias(), object_type.alias(), field_name, to_alias))

    def _add_or_statement(self, object_type: Type[Base], field_name: str, to_alias: str) -> None:
        self._where_clauses.append(
            self._or_statement(
                object_type.alias(),
                object_type.__subclasses__(),
                field_name,
                to_alias,
            )
        )

    def _or_statement(self, from_alias: str, concrete_types: List[Type[Base]], field_name: str, to_alias: str) -> str:
        relationships = [
            self._relationship(from_alias, concrete_type.alias(), field_name, to_alias)
            for concrete_type in concrete_types
        ]

        return f"(or {' '.join(relationships)} )"

    def _relationship(self, from_alias: str, field_type: str, field_name: str, to_alias: str) -> str:
        return f"[ {from_alias} :{field_type}/{field_name} {to_alias} ]"

    def _assert_type(self, object_type: Type[Base]) -> str:
        if not object_type._subclasses():
            return self._to_type_statement(object_type, object_type)

        return f"(or {' '.join([self._to_type_statement(object_type, x) for x in object_type.__subclasses__()])} )"

    def _to_type_statement(self, object_type: Type[Base], other_type: Type[Base]) -> str:
        return f'[ {object_type.alias()} :{TYPE_FIELD} "{other_type.alias()}" ]'

    def _compile_where_clauses(self, where_clauses: List[str], *, separator=" ") -> str:
        """Sorted and deduplicated where clauses, since they are both idempotent and commutative"""

        return separator + separator.join(sorted(set(where_clauses)))

    def _compile_find_clauses(self, find_clauses: List[str]) -> str:
        return " ".join(find_clauses)

    def _compile(self, *, separator=" ") -> str:
        where_clauses = self._where_clauses
        where_clauses.append(self._assert_type(self.result_type))

        if not self._find_clauses:
            find_clauses = self._compile_find_clauses([f"(pull {self.result_type.alias()} [*])"])
        else:
            find_clauses = self._compile_find_clauses(self._find_clauses)

        compiled_where_clauses = self._compile_where_clauses(where_clauses, separator=separator)
        compiled = f"{{:query {{:find [{find_clauses}] :where [{compiled_where_clauses}]"

        if self._limit is not None:
            compiled += f" :limit {self._limit}"

        if self._offset is not None:
            compiled += f" :offset {self._offset}"

        return compiled + "}}"

    def __str__(self) -> str:
        return self._compile()

    def __eq__(self, other):
        return str(self) == str(other)
