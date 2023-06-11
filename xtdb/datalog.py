"""
The Datalog module contains all logic to declaratively create XTDB queries.
"""

from typing import Any, List, Literal, Optional, Tuple, Union

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

    def __invert__(self):
        raise NotImplementedError


class And(Clause):
    def __init__(self, clauses: List[Clause], query_section: str = "where"):
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
        if isinstance(other, (Where, WherePredicate)):
            raise XTDBException("Cannot | on a single where, use & instead")

        return Or([self, other])

    def _and(self, other: Clause) -> Clause:
        if self.query_section != "find" and issubclass(type(other), Find):
            raise XTDBException("Cannot perform a where-find. User find-where instead.")
        if self.query_section == "find" and isinstance(other, And) and other.query_section != "find":
            return FindWhere(self, other)
        if self.query_section == "find" and isinstance(other, (Where, Or, Not, NotJoin, WherePredicate)):
            return FindWhere(self, other)

        return And(self.clauses + [other], self.query_section)

    def __invert__(self):
        return Not(self.clauses)


class Or(Clause):
    def __init__(self, clauses: List[Clause]):
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

    def __invert__(self):
        raise XTDBException("Cannot use ~ on or clauses")


class Not(Clause):
    def __init__(self, clauses: List[Clause]):
        self.clauses = clauses

    def compile(self, root: bool = True, *, separator=" ") -> str:
        collected = []

        for clause in self.clauses:
            collected.append(clause.compile(root=False, separator=separator))

        if all(clause.idempotent for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative for clause in self.clauses):
            collected = sorted(collected)

        if root:
            return f":where [(not{separator}{separator.join(collected)})]"

        return f"(not{separator}{separator.join(collected)})"

    def _or(self, other: Clause) -> Clause:
        return Or(self.clauses + [other])

    def __invert__(self):
        return And(self.clauses)


class NotJoin(Clause):
    def __init__(self, variable: str, clauses: Optional[List] = None):
        self.variable = variable
        self.clauses = clauses or []

    def compile(self, root: bool = True, *, separator=" ") -> str:
        collected = []

        for clause in self.clauses:
            collected.append(clause.compile(root=False, separator=separator))

        if all(clause.idempotent for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative for clause in self.clauses):
            collected = sorted(collected)

        if root:
            return f":where [(not-join{separator}[{self.variable}] {separator.join(collected)})]"

        return f"(not-join{separator}[{self.variable}] {separator.join(collected)})"

    def _and(self, other: Clause) -> Clause:
        return NotJoin(self.variable, self.clauses + [other])

    def __invert__(self):
        raise XTDBException("Cannot use ~ on not-join")


class OrJoin(Clause):
    def __init__(self, variable: str, clauses: Optional[List] = None):
        self.variable = variable
        self.clauses = clauses or []

    def compile(self, root: bool = True, *, separator=" ") -> str:
        collected = []

        for clause in self.clauses:
            collected.append(clause.compile(root=False, separator=separator))

        if all(clause.idempotent for clause in self.clauses):
            collected = list(set(collected))

        if all(clause.commutative for clause in self.clauses):
            collected = sorted(collected)

        if root:
            return f":where [(or-join{separator}[{self.variable}] {separator.join(collected)})]"

        return f"(or-join{separator}[{self.variable}] {separator.join(collected)})"

    def _and(self, other: Clause) -> Clause:
        return OrJoin(self.variable, self.clauses + [other])

    def __invert__(self):
        raise XTDBException("Cannot use ~ on or-join")


class Where(Clause):
    def __init__(self, document: str, field: str, value: Any = ""):
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

    def __invert__(self):
        return Not([self])


class WherePredicate(Clause):
    def __init__(self, operation: str, *args, bind: Optional[str] = None):
        self.args = args
        self.operation = operation
        self.bind = bind

    def compile(self, root: bool = True, *, separator=" ") -> str:
        bind = self.bind or ""

        if root:
            return f":where [[ ({self.operation} {' '.join([str(arg) for arg in self.args])}) {bind}]]"

        return f"[ ({self.operation} {' '.join(self.args)}) {bind}]"

    def _or(self, other: Clause) -> Clause:
        if isinstance(other, And):
            raise XTDBException("Cannot | on a single predicate, use & instead")

        return Or([self, other])

    def __invert__(self):
        return Not([self])


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

    def __invert__(self):
        raise XTDBException("Cannot use ~ on query keys")


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
        if isinstance(other, (Where, Or, Not, WherePredicate)):
            return FindWhere(self, other)

        return And([self, other], "find")


class In(QueryKey):
    def __init__(self, in_args: Union[str, List[str], List[List[str]]], values: Union[str, List[str], List[List[str]]]):
        if not in_args:
            raise XTDBException("No in_arg supplied: cannot be empty")
        if not values:
            raise XTDBException("No values supplied: cannot be empty")

        self.in_args = in_args
        self.values = values

    def compile(self, root: bool = True, *, separator=" ") -> str:
        if isinstance(self.in_args, str):
            return f" :in [{self.in_args}]"

        if isinstance(self.in_args[0], str):
            expression = " ".join([in_arg for in_arg in self.in_args if isinstance(in_arg, str)])
            return f" :in [[{expression}]]"

        nested_args = [" ".join(in_arg) for in_arg in self.in_args if isinstance(in_arg, List)]
        expression = " ".join(nested_args)

        return f" :in [[[{expression}]]]"

    def compile_values(self) -> str:
        if not isinstance(self.values, List):
            return f' :in-args ["{self.values}"]'

        if not isinstance(self.values[0], List):
            expression = " ".join([f'"{value}"' for value in self.values])
            return f" :in-args [[{expression}]]"

        nested_values = ["[" + " ".join([f'"{value}"' for value in values]) + "]" for values in self.values]
        expression = " ".join(nested_values)
        return f" :in-args [[{expression}]]"


class OrderBy(QueryKey):
    def __init__(self, fields: List[Tuple[str, Literal["asc", "desc"]]]):
        if not all([field[1] in ["asc", "desc"] for field in fields]):
            raise XTDBException("Only 'asc' and 'desc' allowed as ordering functions.")

        self.fields = fields

    def compile(self, root: bool = True, *, separator=" ") -> str:
        expression = " ".join([self.compile_field(field) for field in self.fields])

        return f" :order-by [{expression}]"

    def compile_field(self, field: Tuple[str, Literal["asc", "desc"]]):
        return f"[{field[0]} :{field[1]}]"


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
        in_args: Optional[In] = None,
        order_by: Optional[OrderBy] = None,
        limit: Optional[Limit] = None,
        offset: Optional[Offset] = None,
        timeout: Optional[Timeout] = None,
    ):
        self.find = find
        self.where = where
        self.in_args = in_args
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.timeout = timeout

    def compile(self, root: bool = True, *, separator=" ") -> str:
        q = f"{{:query {{{self.find.compile(separator=separator)} {self.where.compile(separator=separator)}"

        if self.in_args is not None:
            q += self.in_args.compile(separator=separator)

        if self.order_by is not None:
            q += self.order_by.compile(separator=separator)

        if self.limit is not None:
            q += self.limit.compile(separator=separator)

        if self.offset is not None:
            q += self.offset.compile(separator=separator)

        if self.timeout is not None:
            q += self.timeout.compile(separator=separator)

        if self.in_args is not None:
            return q + f"}}{self.in_args.compile_values()}}}"

        return q + "}}"

    def _and(self, other: Clause) -> Clause:
        if isinstance(other, In):
            return FindWhere(self.find, self.where, other, self.order_by, self.limit, self.offset, self.timeout)
        if isinstance(other, OrderBy):
            return FindWhere(self.find, self.where, self.in_args, other, self.limit, self.offset, self.timeout)
        if isinstance(other, Limit):
            return FindWhere(self.find, self.where, self.in_args, self.order_by, other, self.offset, self.timeout)
        if isinstance(other, Offset):
            return FindWhere(self.find, self.where, self.in_args, self.order_by, self.limit, other, self.timeout)
        if isinstance(other, Timeout):
            return FindWhere(self.find, self.where, self.in_args, self.order_by, self.limit, self.offset, other)

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
