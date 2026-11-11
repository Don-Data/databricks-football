"""Give the prod service principal the access it needs. Run once, as a workspace admin.

`databricks bundle run grant_prod_access -t prod` calls this (see `scripts` in databricks.yml).
It's the bootstrap GitHub Actions can't do for itself: before its first deploy, the service
principal can't access anything. It's safe to run again.

The bundle resolves these environment variables from the prod target:

    SP_APPLICATION_ID   the service principal, looked up by name
    CATALOG             USE CATALOG
    SCHEMAS             ALL PRIVILEGES on each (comma-separated): write the landing volume,
                        build the pipeline's tables. Dev schemas stay closed.
    SECRET_SCOPE        READ: the job reads the API token
    JOB_NAME            ownership (IS_OWNER) of the existing prod job

No warehouse grant: the bundle looks the Serverless Starter Warehouse up by name on every
deploy, and the `users` group, which includes the service principal, already has CAN_USE.

Why ownership and not just CAN_MANAGE: a deploy makes the deployer the owner, and only an
admin or the current owner may change the owner. So the service principal can only deploy
the job once it already owns it. Deploy right afterwards: from the transfer on, the job runs
as the service principal, which can't read the files of the previous deployment.

The pipeline can't be handed over the same way: "Only metastore admins can change pipeline
owner", and on Free Edition the metastore belongs to Databricks. The README describes how the
service principal creates it anew instead.

The admin running this also gets read access to the prod schemas: once the service principal
owns the tables, owning the schema isn't enough to query them.
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel
from databricks.sdk.service.workspace import AclPermission

READ = [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.READ_VOLUME]


def only(ids: list[str], what: str) -> str:
    if len(ids) != 1:
        raise SystemExit(f"expected exactly one {what}, found {len(ids)}")
    return ids[0]


def main() -> None:
    sp = os.environ["SP_APPLICATION_ID"]
    if not sp:
        raise SystemExit("no service principal: run this with -t prod")
    catalog = os.environ["CATALOG"]
    w = WorkspaceClient()  # authenticated through the environment that `bundle run` sets
    me = w.current_user.me().user_name

    w.grants.update("catalog", catalog, changes=[PermissionsChange(principal=sp, add=[Privilege.USE_CATALOG])])
    print(f"catalog {catalog}: USE CATALOG")

    for schema in os.environ["SCHEMAS"].split(","):
        w.grants.update(
            "schema",
            f"{catalog}.{schema}",
            changes=[PermissionsChange(principal=sp, add=[Privilege.ALL_PRIVILEGES]), PermissionsChange(principal=me, add=READ)],
        )
        print(f"schema {catalog}.{schema}: ALL PRIVILEGES for the service principal, read for you")

    scope = os.environ["SECRET_SCOPE"]
    w.secrets.put_acl(scope, sp, AclPermission.READ)
    print(f"secret scope {scope}: READ")

    # The admin running this keeps CAN_MANAGE until the deploy sets the bundle's permissions.
    job_name = os.environ["JOB_NAME"]
    job_id = only([str(j.job_id) for j in w.jobs.list(name=job_name) if j.job_id is not None], f"job named {job_name}")
    w.jobs.update_permissions(
        job_id,
        access_control_list=[
            JobAccessControlRequest(service_principal_name=sp, permission_level=JobPermissionLevel.IS_OWNER),
            JobAccessControlRequest(user_name=me, permission_level=JobPermissionLevel.CAN_MANAGE),
        ],
    )
    print(f"job {job_name}: owned by the service principal")


if __name__ == "__main__":
    main()
