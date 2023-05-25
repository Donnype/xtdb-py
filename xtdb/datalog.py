from typing import Any, Tuple, Union

from xtdb.exceptions import XTDBException


class Clause:
    @classmethod
    def commutative(cls):
        return True

    @classmethod
    def idempotent(cls):
        return True

    def compile(self) -> str:
        raise NotImplementedError

    def _collect(self) -> Tuple:
        return (self.compile(),)

    def __str__(self) -> str:
        return self.compile()

    def __and__(self, other: "Clause") -> "Clause":
        return And(self, other)

    def __or__(self, other: "Clause") -> "Clause":
        raise NotImplementedError


class And(Clause):
    def __init__(self, clause: Clause, other: Clause):
        super().__init__()

        self.clause = clause
        self.other = other

    def compile(self) -> str:
        collected = self._collect()

        if self.clause.idempotent() and self.other.idempotent():
            collected = tuple(set(collected))

        if self.clause.commutative() and self.other.commutative():
            return " ".join(sorted(collected))

        return " ".join(reversed(collected))

    def _collect(self) -> Tuple:
        return self.other._collect() + self.clause._collect()

    def __or__(self, other: Clause) -> Clause:
        return And(self, Or(other))


class Or(Clause):
    def __init__(self, clause: Clause):
        super().__init__()

        self.clause = clause

    def compile(self) -> str:
        return f"(or {self.clause.compile()})"

    def __or__(self, other: Clause) -> Clause:
        return Or(And(self.clause, other))


class Where(Clause):
    def __init__(self, document: str, field: str, value: Any):
        super().__init__()

        self.document = document
        self.field = field
        self.value = value

    def compile(self) -> str:
        return f"[ {self.document} :{self.field} {self.value} ]"

    def __or__(self, other: Clause) -> Clause:
        if isinstance(other, And):
            return And(Or(self), other)

        return Or(And(self, other))


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

    def compile(self) -> str:
        return str(self.expression)

    def __or__(self, other: Clause) -> Clause:
        raise XTDBException("Or operator is not supported for find clauses")
