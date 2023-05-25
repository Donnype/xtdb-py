from typing import Any


class Clause:
    def compile(self) -> str:
        raise NotImplementedError

    def _collect(self) -> set:
        return {self.compile()}

    def __str__(self) -> str:
        return self.compile()

    def __and__(self, other: "Clause") -> "Clause":
        return And(self, other)

    def __or__(self, other: "Clause") -> "Clause":
        raise NotImplementedError


class And(Clause):
    def __init__(self, clause: Clause, other: Clause):
        self.clause = clause
        self.other = other

    def compile(self) -> str:
        return " ".join(sorted(self._collect()))

    def _collect(self) -> set:
        return self.clause._collect().union(self.other._collect())

    def __or__(self, other: Clause) -> Clause:
        return And(self, Or(other))


class Or(Clause):
    def __init__(self, clause: Clause):
        self.clause = clause

    def compile(self) -> str:
        return f"(or {self.clause.compile()})"

    def __or__(self, other: Clause) -> Clause:
        return Or(And(self.clause, other))


class Where(Clause):
    def __init__(self, document: str, field: str, value: Any):
        self.document = document
        self.field = field
        self.value = value

    def compile(self) -> str:
        return f"[ {self.document} :{self.field} {self.value} ]"

    def __or__(self, other: Clause) -> Clause:
        if isinstance(other, And):
            return And(Or(self), other)

        return Or(And(self, other))
