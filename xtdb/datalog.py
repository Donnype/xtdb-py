from typing import Any, List, Optional, Union

from xtdb.exceptions import XTDBException


class Clause:
    @classmethod
    def commutative(cls):
        return True

    @classmethod
    def idempotent(cls):
        return True

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

        if issubclass(type(other), Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")

        return And([self, other])

    def __or__(self, other: Optional["Clause"]) -> "Clause":
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
        collected = self._collect(separator=separator)

        if all(clause.idempotent() for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative() for clause in self.clauses):
            expression = separator + separator.join(sorted(collected))
        else:
            expression = separator + separator.join(collected)

        if root:
            return f":{self.query_section} [{expression}]"

        return expression

    def _collect(self, *, separator=" ") -> List:
        collected = []

        for clause in self.clauses:
            collected.extend(clause._collect(separator=separator))

        return collected

    def __or__(self, other: Optional[Clause]) -> "Clause":
        if other is None:
            return self

        if isinstance(other, Where):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or([self, other])

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

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

        if all(clause.idempotent() for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative() for clause in self.clauses):
            expression = separator + separator.join(sorted(collected))
        else:
            expression = separator + separator.join(collected)

        if root:
            return f":where [(or{expression})]"

        return f"(or{expression})"

    def __or__(self, other: Optional[Clause]) -> "Clause":
        if other is None:
            return self

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

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        if isinstance(other, And):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or([self, other])


class Expression:
    def __init__(self, statement: str):
        self.statement = statement

    def __str__(self):
        return self.statement


class Aggregate(Expression):
    supported_aggregates = {
        "sum": (),
        "min": (),
        "max": (),
        "count": (),
        "avg": (),
        "median": (),
        "variance": (),
        "stddev": (),
        "rand": ("N",),
        "sample": ("N",),
        "distinct": (),
    }

    def __init__(self, function: str, expression: str, *args):
        if function not in self.supported_aggregates:
            raise XTDBException("Invalid aggregate function")

        if len(self.supported_aggregates[function]) != len(args):
            raise XTDBException(
                f"Invalid arguments to aggregate function, needs: {self.supported_aggregates[function]}",
            )

        if self.supported_aggregates[function]:
            statement = f"({function} {' '.join(args)} {expression})"
        else:
            statement = f"({function} {expression})"

        super().__init__(statement)


class Find(Clause):
    def __init__(self, expression: Union[str, Expression]):
        self.expression = expression

    @classmethod
    def commutative(cls):
        return False

    @classmethod
    def idempotent(cls):
        return False

    def compile(self, root: bool = True, *, separator=" ") -> str:
        if root:
            return f":find [{self.expression}]"

        return str(self.expression)

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        raise XTDBException("Or operator is not supported for find clauses")

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        if isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)

        if isinstance(other, Where):
            return FindWhere(self, other)

        return And([self, other], "find")


class Limit(Clause):
    def __init__(self, limit: int):
        self.limit = limit

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :limit {self.limit}"

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        return And([self, other])

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        raise XTDBException("Cannot use | on query keys")


class Offset(Clause):
    def __init__(self, offset: int):
        self.offset = offset

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :offset {self.offset}"

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        return And([self, other])

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        raise XTDBException("Cannot use | on query keys")


class Timeout(Clause):
    def __init__(self, timeout: int):
        self.timeout = timeout

    def compile(self, root: bool = True, *, separator=" ") -> str:
        return f" :timeout {self.timeout}"

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        return And([self, other])

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        raise XTDBException("Cannot use | on query keys")


class FindWhere(Clause):
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

    def __or__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        raise XTDBException("Or operator is not supported for find-where clauses")

    def __and__(self, other: Optional[Clause]) -> Clause:
        if other is None:
            return self

        if isinstance(other, Limit):
            return self.__class__(self.find, self.where, other, self.offset, self.timeout)

        if isinstance(other, Offset):
            return self.__class__(self.find, self.where, self.limit, other, self.timeout)

        if isinstance(other, Timeout):
            return self.__class__(self.find, self.where, self.limit, self.offset, other)

        raise XTDBException("And operator is not supported for find-where clauses")


class Sum(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("sum", expression)
        super().__init__(aggregate)


class Min(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("min", expression)
        super().__init__(aggregate)


class Max(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("max", expression)
        super().__init__(aggregate)


class Count(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("count", expression)
        super().__init__(aggregate)


class CountDistinct(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("count-distinct", expression)
        super().__init__(aggregate)


class Avg(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("avg", expression)
        super().__init__(aggregate)


class Median(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("median", expression)
        super().__init__(aggregate)


class Variance(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("variance", expression)
        super().__init__(aggregate)


class Stddev(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("stddev", expression)
        super().__init__(aggregate)


class Distinct(Find):
    def __init__(self, expression: str):
        aggregate = Aggregate("distinct", expression)
        super().__init__(aggregate)


class Rand(Find):
    def __init__(self, expression: str, N: int):
        aggregate = Aggregate("rand", expression, str(N))
        super().__init__(aggregate)


class Sample(Find):
    def __init__(self, expression: str, N: int):
        aggregate = Aggregate("sample", expression, str(N))
        super().__init__(aggregate)
