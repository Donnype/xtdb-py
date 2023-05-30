from typing import Any, List, Optional, Union

from xtdb.exceptions import XTDBException


class Clause:
    commutative = True
    idempotent = True

    def compile(self, root: bool = True, *, separator=" ") -> str:
        raise NotImplementedError

    def format(self) -> str:
        return self.compile(separator="\n    ")

    def _collect(self, *, separator=" ") -> List:
        return [self.compile(root=False, separator=separator)]

    def __str__(self) -> str:
        return self.compile()

    def __and__(self, other: Optional["Clause"]) -> "Clause":
        if other is None:
            return self

        return self._and(other)

    def _and(self, other: "Clause") -> "Clause":
        if issubclass(type(other), Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")

        return And([self, other])

    def __or__(self, other: Optional["Clause"]) -> "Clause":
        if other is None:
            return self

        return self._or(other)

    def _or(self, other: "Clause") -> "Clause":
        raise NotImplementedError

    def __ror__(self, other: Optional["Clause"]) -> "Clause":
        if other is None:
            return self

        raise NotImplementedError

    def __rand__(self, other: Optional["Clause"]) -> "Clause":
        if other is None:
            return self

        raise NotImplementedError


class And(Clause):
    def __init__(self, clauses: List[Clause], query_section: str = "where"):
        super().__init__()

        self.clauses = clauses
        self.query_section = query_section

    def compile(self, root: bool = True, *, separator=" ") -> str:
        compiled_clauses = self._collect(separator=separator)
        expression = separator + separator.join(compiled_clauses)

        if root:
            return f":{self.query_section} [{expression}]"

        return expression

    def _collect(self, *, separator=" ") -> List:
        collected = []

        for clause in self.clauses:
            collected.extend(clause._collect(separator=separator))

        if all(clause.idempotent for clause in self.clauses):
            collected = list(set(collected))
        if all(clause.commutative for clause in self.clauses):
            collected = sorted(collected)

        return collected

    def _or(self, other: Clause) -> "Clause":
        if isinstance(other, Where):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or([self, other])

    def _and(self, other: Clause) -> Clause:
        if self.query_section != "find" and issubclass(type(other), Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")
        if self.query_section == "find" and isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)
        if self.query_section == "find" and isinstance(other, Where):
            return FindWhere(self, other)

        return And(self.clauses + [other], self.query_section)


class Or(Clause):
    def __init__(self, clauses: List[Clause]):
        super().__init__()

        self.clauses = clauses

    def compile(self, root: bool = True, *, separator=" ") -> str:
        collected = []

        for clause in self.clauses:
            if isinstance(clause, And):
                collected.append(f"(and{clause.compile(root=False, separator=separator)})")
            else:
                collected.append(clause.compile(root=False, separator=separator))

        if all(clause.idempotent for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative for clause in self.clauses):
            collected = sorted(collected)

        if root:
            return f":where [(or{separator}{separator.join(collected)})]"

        return f"(or{separator}{separator.join(collected)})"

    def _or(self, other: Clause) -> Clause:
        return Or(self.clauses + [other])


class Where(Clause):
    def __init__(self, document: str, field: str, value: Any = ""):
        super().__init__()

        self.document = document
        self.field = field
        self.value = value

    def compile(self, root: bool = True, *, separator=" ") -> str:
        if root:
            return f":where [[ {self.document} :{self.field} {self.value} ]]"

        return f"[ {self.document} :{self.field} {self.value} ]"

    def _or(self, other: Clause) -> Clause:
        if isinstance(other, And):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or([self, other])


class Expression:
    def __init__(self, statement: str):
        self.statement = statement

    def __str__(self):
        return self.statement


class _BaseAggregate(Expression):
    supported_aggregates = ["sum", "min", "max", "count", "avg", "median", "variance", "stddev", "distinct"]
    supported_aggregates_with_arg = ["rand", "sample"]

    def __init__(self, function: str, expression: str, *args):
        if function not in self.supported_aggregates + self.supported_aggregates_with_arg:
            raise XTDBException("Invalid aggregate function")

        if function in self.supported_aggregates:
            super().__init__(f"({function} {expression})")

        if function in self.supported_aggregates_with_arg:
            if len(args) != 1:
                raise XTDBException("Invalid arguments to aggregate, it needs one argument: N")

            super().__init__(f"({function} {' '.join(args)} {expression})")


class QueryKey(Clause):
    def compile(self, root: bool = True, *, separator=" ") -> str:
        raise NotImplementedError

    def _or(self, other: Clause) -> Clause:
        raise XTDBException("Cannot use | on query keys")

    def _and(self, other: Clause) -> Clause:
        return And([self, other])


class Find(QueryKey):
    commutative = False
    idempotent = False

    def __init__(self, expression: Union[str, Expression]):
        self.expression = expression

    def compile(self, root: bool = True, *, separator=" ") -> str:
        if root:
            return f":find [{self.expression}]"

        return str(self.expression)

    def _and(self, other: Clause) -> Clause:
        if isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)
        if isinstance(other, Where):
            return FindWhere(self, other)

        return And([self, other], "find")


class Limit(QueryKey):
    def __init__(self, limit: int):
        self.limit = limit

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :limit {self.limit}"


class Offset(QueryKey):
    def __init__(self, offset: int):
        self.offset = offset

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :offset {self.offset}"


class Timeout(QueryKey):
    def __init__(self, timeout: int):
        self.timeout = timeout

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :timeout {self.timeout}"


class FindWhere(QueryKey):
    def __init__(
        self,
        find: Clause,
        where: Clause,
        limit: Optional[Limit] = None,
        offset: Optional[Offset] = None,
        timeout: Optional[Timeout] = None,
    ):
        self.find = find
        self.where = where
        self.limit = limit
        self.offset = offset
        self.timeout = timeout

    def compile(self, root: bool = True, *, separator=" ") -> str:
        q = f"{{:query {{{self.find.compile(separator=separator)} {self.where.compile(separator=separator)}"

        if self.limit is not None:
            q += self.limit.compile(separator=separator)

        if self.offset is not None:
            q += self.offset.compile(separator=separator)

        if self.timeout is not None:
            q += self.timeout.compile(separator=separator)

        return q + "}}"

    def _and(self, other: Clause) -> Clause:
        if isinstance(other, Limit):
            return FindWhere(self.find, self.where, other, self.offset, self.timeout)
        if isinstance(other, Offset):
            return FindWhere(self.find, self.where, self.limit, other, self.timeout)
        if isinstance(other, Timeout):
            return FindWhere(self.find, self.where, self.limit, self.offset, other)

        raise XTDBException("And operator is not supported for find-where clauses")


def _build_find_aggregation_class(name: str):
    class Extended(Find):
        def __init__(self, expression: str):
            super().__init__(_BaseAggregate(name, expression))

    return Extended


def _build_find_aggregation_class_with_argument(name: str):
    class Extended(Find):
        def __init__(self, expression: str, N: int):
            super().__init__(_BaseAggregate(name, expression, str(N)))

    return Extended


# Dynamically create classes extending the Find clause but do aggregations
Sum = _build_find_aggregation_class("sum")
Min = _build_find_aggregation_class("min")
Max = _build_find_aggregation_class("max")
Count = _build_find_aggregation_class("count")
CountDistinct = _build_find_aggregation_class("count-distinct")
Avg = _build_find_aggregation_class("avg")
Median = _build_find_aggregation_class("median")
Variance = _build_find_aggregation_class("variance")
Stddev = _build_find_aggregation_class("stddev")
Distinct = _build_find_aggregation_class("distinct")
Rand = _build_find_aggregation_class_with_argument("rand")
Sample = _build_find_aggregation_class_with_argument("sample")
