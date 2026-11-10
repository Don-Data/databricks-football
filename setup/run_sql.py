"""Run a SQL file on a SQL warehouse, one statement at a time.

`databricks bundle run setup` calls this (see `scripts` in databricks.yml). The bundle
provides the authentication and these environment variables:

    SQL_WAREHOUSE_ID    the warehouse to run on
    SQL_PARAM_<NAME>    the value of the named parameter :<name>, e.g. SQL_PARAM_CATALOG for :catalog

Statements are split at ";", so keep semicolons out of string literals and comments.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Mapping
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

PARAM_PREFIX = "SQL_PARAM_"
FINISHED = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


def split_statements(sql: str) -> list[str]:
    """Split at ";" and drop the pieces that hold nothing but comments and blank lines."""
    statements: list[str] = []
    for piece in sql.split(";"):
        code = [line for line in piece.splitlines() if line.strip() and not line.strip().startswith("--")]
        if code:
            statements.append("\n".join(code))
    return statements


def parameters_from_env(env: Mapping[str, str]) -> list[StatementParameterListItem]:
    return [
        StatementParameterListItem(name=key.removeprefix(PARAM_PREFIX).lower(), value=value)
        for key, value in env.items()
        if key.startswith(PARAM_PREFIX)
    ]


def run(w: WorkspaceClient, warehouse_id: str, statement: str, parameters: list[StatementParameterListItem]) -> None:
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, parameters=parameters, wait_timeout="30s"
    )
    # A stopped warehouse needs a moment to start, so keep polling until the statement finishes.
    while response.status is None or response.status.state not in FINISHED:
        time.sleep(5)
        response = w.statement_execution.get_statement(str(response.statement_id))
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error.message if response.status.error else "no error message"
        raise SystemExit(f"{response.status.state}: {error}")


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise SystemExit("usage: run_sql.py FILE.sql")

    warehouse_id = os.environ["SQL_WAREHOUSE_ID"]
    parameters = parameters_from_env(os.environ)
    w = WorkspaceClient()  # authenticated through the environment that `bundle run` sets

    for statement in split_statements(Path(args[0]).read_text(encoding="utf-8")):
        print(statement.splitlines()[0], " ".join(f"[{p.name}={p.value}]" for p in parameters))
        run(w, warehouse_id, statement, parameters)
    print("done")


if __name__ == "__main__":
    main()
