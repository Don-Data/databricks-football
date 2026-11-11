"""Give the prod service principal what it needs before its first deploy. Run once, as an admin.

`databricks bundle run grant_prod_access -t prod` calls this (see `scripts` in databricks.yml).
GitHub Actions deploys prod as the service principal, and the service principal creates every
prod object itself: the schemas, the landing volume, the job, the pipeline and its tables.
So it owns them, and needs grants only for what exists before its first deploy:

    SP_APPLICATION_ID   the service principal, looked up by name
    CATALOG             USE CATALOG and CREATE SCHEMA (`bundle run setup` creates the catalog)
    SECRET_SCOPE        READ: the job reads the API token from it

The operator's read access to prod isn't here. It lives in databricks.yml as `grants` on the prod
schemas, and the service principal sets them itself as the schemas' owner.

No warehouse grant: the bundle looks the Serverless Starter Warehouse up by name on every deploy,
and the `users` group, which includes the service principal, already has CAN_USE.

Safe to run again: every grant only adds.
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.workspace import AclPermission


def main() -> None:
    sp = os.environ["SP_APPLICATION_ID"]
    if not sp:
        raise SystemExit("no service principal: run this with -t prod")
    w = WorkspaceClient()  # authenticated through the environment that `bundle run` sets

    catalog = os.environ["CATALOG"]
    w.grants.update(
        "catalog",
        catalog,
        changes=[PermissionsChange(principal=sp, add=[Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA])],
    )
    print(f"catalog {catalog}: USE CATALOG, CREATE SCHEMA")

    scope = os.environ["SECRET_SCOPE"]
    w.secrets.put_acl(scope, sp, AclPermission.READ)
    print(f"secret scope {scope}: READ")


if __name__ == "__main__":
    main()
