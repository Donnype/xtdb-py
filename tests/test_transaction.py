import json

from xtdb.session import Operation, OperationType, Transaction


def test_transaction_json(valid_time):
    transaction = Transaction()
    transaction.add(Operation(type=OperationType.MATCH, value={"xt/id": "value"}, valid_time=valid_time))
    transaction.add(Operation(type=OperationType.DELETE, value="value", valid_time=valid_time))
    transaction.add(Operation(type=OperationType.PUT, value={"xt/id": "value"}, valid_time=valid_time))
    transaction.add(Operation(type=OperationType.EVICT, value="value", valid_time=valid_time))

    assert json.loads(transaction.json()) == json.loads(
        f"""{{"tx-ops": [
        ["match", "value", {{"xt/id": "value"}}, "{valid_time.isoformat()}"],
        ["delete", "value", "{valid_time.isoformat()}"],
        ["put", {{"xt/id": "value"}}, "{valid_time.isoformat()}"],
        ["evict", "value", "{valid_time.isoformat()}"]
]}}"""
    )


def test_transaction_json_with_methods(valid_time):
    transaction = Transaction()
    transaction.add(Operation.match({"xt/id": "value"}, valid_time))
    transaction.add(Operation.delete("value", valid_time))
    transaction.add(Operation.put({"xt/id": "value"}, valid_time))
    transaction.add(Operation.evict("value", valid_time))

    assert json.loads(transaction.json()) == json.loads(
        f"""{{"tx-ops": [
        ["match", "value", {{"xt/id": "value"}}, "{valid_time.isoformat()}"],
        ["delete", "value", "{valid_time.isoformat()}"],
        ["put", {{"xt/id": "value"}}, "{valid_time.isoformat()}"],
        ["evict", "value", "{valid_time.isoformat()}"]
]}}"""
    )
