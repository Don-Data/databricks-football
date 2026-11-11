"""Give the prod service principal the access it needs. Run once, as a workspace admin.

`databricks bundle run grant_prod_access -t prod` calls this (see `scripts` in databricks.yml).
It's the bootstrap GitHub Actions can't do for itself: before its first deploy, the service
principal can't access anything. It's safe to run again.

The bundle resolves these environment variables from the prod target:

    SP_APPLICATION_ID         the service principal, looked up by name
    CATALOG                   USE CATALOG
    SCHEMAS                   ALL PRIVILEGES on each (comma-separated): write the landing
                              volume, build the pipeline's tables. Dev schemas stay closed.
    SECRET_SCOPE              READ: the job reads the API token
    WAREHOUSE_ID              CAN_USE: the bundle looks the warehouse up by name
    JOB_NAME, PIPELINE_NAME   ownership (IS_OWNER) of the existing prod job and pipeline

Why ownership and not just CAN_MANAGE: a deploy makes the deployer the owner, and only an
admin or the current owner may change the owner. So the service principal can only deploy
once it already owns them. Deploy right afterwards: from the transfer on, the job and pipeline
run as the service principal, which can't read the files of the previous deployment.
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel
from databricks.sdk.service.pipelines import (
    PipelineAccessControlRequest,
    PipelinePermissionLevel,
)
from databricks.sdk.service.sql import (
    WarehouseAccessControlRequest,
    WarehousePermissionLevel,
)
from databricks.sdk.service.workspace import AclPermission


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

    w.grants.update("catalog", catalog, changes=[PermissionsChange(principal=sp, add=[Privilege.USE_CATALOG])])
    print(f"catalog {catalog}: USE CATALOG")

    for schema in os.environ["SCHEMAS"].split(","):
        w.grants.update(
            "schema", f"{catalog}.{schema}", changes=[PermissionsChange(principal=sp, add=[Privilege.ALL_PRIVILEGES])]
        )
        print(f"schema {catalog}.{schema}: ALL PRIVILEGES")

    scope = os.environ["SECRET_SCOPE"]
    w.secrets.put_acl(scope, sp, AclPermission.READ)
    print(f"secret scope {scope}: READ")

    w.warehouses.update_permissions(
        os.environ["WAREHOUSE_ID"],
        access_control_list=[
            WarehouseAccessControlRequest(service_principal_name=sp, permission_level=WarehousePermissionLevel.CAN_USE)
        ],
    )
    print("warehouse: CAN USE")

    # The admin running this keeps CAN_MANAGE until the deploy sets the bundle's permissions.
    me = w.current_user.me().user_name

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

    pipeline_name = os.environ["PIPELINE_NAME"]
    pipelines = w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")
    pipeline_id = only(
        [p.pipeline_id for p in pipelines if p.name == pipeline_name and p.pipeline_id], f"pipeline named {pipeline_name}"
    )
    w.pipelines.update_permissions(
        pipeline_id,
        access_control_list=[
            PipelineAccessControlRequest(service_principal_name=sp, permission_level=PipelinePermissionLevel.IS_OWNER),
            PipelineAccessControlRequest(user_name=me, permission_level=PipelinePermissionLevel.CAN_MANAGE),
        ],
    )
    print(f"pipeline {pipeline_name}: owned by the service principal")


if __name__ == "__main__":
    main()
