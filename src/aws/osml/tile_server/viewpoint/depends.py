from typing import Annotated

from fastapi import Depends, HTTPException

from aws.osml.tile_server.viewpoint.database import ViewpointStatusTable

GLOBAL_STATUS_TABLE = ViewpointStatusTable()

async def get_viewpoint_status_table() -> ViewpointStatusTable:
    return GLOBAL_STATUS_TABLE

ViewpointStatusTableDep = Annotated[ViewpointStatusTable, Depends(get_viewpoint_status_table)]
