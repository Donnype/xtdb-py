from typing import Any, Tuple, Union

from xtdb.exceptions import XTDBException


class Clause:
    @classmethod
    def commutative(cls):
        return True

    @classmethod
    def idempotent(cls):
        return True

    def compile(self, root: bool = True) -> str:
        raise NotImplementedError

    def _collect(self) -> Tuple:
        return (self.compile(root=False),)

    def __str__(self) -> str:
        return self.compile()

    def __and__(self, other: "Clause") -> "Clause":
        if isinstance(other, Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")

        return And(self, other)

    def __or__(self, other: "Clause") -> "Or":
        raise NotImplementedError


class And(Clause):
    def __init__(self, clause: Clause, other: Clause, query_section: str = "where"):
        super().__init__()

        self.clause = clause
        self.other = other
        self.query_section = query_section

    def compile(self, root: bool = True) -> str:
        collected = self._collect()

        if self.clause.idempotent() and self.other.idempotent():
            collected = tuple(set(collected))

        if self.clause.commutative() and self.other.commutative():
            expression = " ".join(sorted(collected))
        else:
            expression = " ".join(reversed(collected))

        if root:
            return f":{self.query_section} [{expression}]"

        return expression

    def _collect(self) -> Tuple:
        return self.other._collect() + self.clause._collect()

    def __or__(self, other: Clause) -> Clause:
        if isinstance(other, Where):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or(self, other, self.query_section)

    def __and__(self, other: Clause) -> Clause:
        if self.query_section != "find" and isinstance(other, Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")
        elif isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)

        return And(self, other, self.query_section)


class Or(Clause):
    def __init__(self, clause: Clause, other: Clause, query_section: str = "where"):
        super().__init__()

        self.clause = clause
        self.other = other
        self.query_section = query_section

    def compile(self, root: bool = True) -> str:
        collected = ()

        if isinstance(self.clause, And):
            collected = collected + (f"(and {self.clause.compile(root=False)})",)
        else:
            collected = collected + (self.clause.compile(root=False),)

        if isinstance(self.other, And):
            collected = collected + (f"(and {self.other.compile(root=False)})",)
        else:
            collected = collected + (self.other.compile(root=False),)

        if self.clause.idempotent() and self.other.idempotent():
            collected = tuple(set(collected))

        if self.clause.commutative() and self.other.commutative():
            expression = " ".join(sorted(collected))
        else:
            expression = " ".join(reversed(collected))

        if root:
            return f":where [(or {expression})]"

        return f"(or {expression})"

    def __or__(self, other: Clause) -> "Or":
        return Or(self, other, self.query_section)


class Where(Clause):
    def __init__(self, document: str, field: str, value: Any):
        super().__init__()

        self.document = document
        self.field = field
        self.value = value

    def compile(self, root: bool = True) -> str:
        if root:
            return f":where [[ {self.document} :{self.field} {self.value} ]]"

        return f"[ {self.document} :{self.field} {self.value} ]"

    def __or__(self, other: Clause) -> Or:
        if isinstance(other, And):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or(self, other)


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

    def compile(self, root: bool = True) -> str:
        if root:
            return f":find [{self.expression}]"

        return str(self.expression)

    def __or__(self, other: Clause) -> Clause:
        raise XTDBException("Or operator is not supported for find clauses")

    def __and__(self, other: Clause) -> Clause:
        if isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)

        if isinstance(other, Where):
            return FindWhere(self, other)

        return And(self, other, "find")


class FindWhere(Clause):
    def __init__(self, find: Clause, where: Clause):
        self.find = find
        self.where = where

    def compile(self, root: bool = True) -> str:
        return f"{{{self.find} {self.where}}}"

    def __or__(self, other: "Clause") -> "Clause":
        raise XTDBException("Or operator is not supported for find-where clauses")

    def __and__(self, other: "Clause") -> "Clause":
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
