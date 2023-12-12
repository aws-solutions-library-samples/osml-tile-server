import logging
from enum import auto
from typing import List, Optional

from pydantic import BaseModel, Field

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.utils import AutoLowerStringEnum, AutoUnderscoreStringEnum

logger = logging.getLogger("uvicorn")


class ViewpointApiNames(str, AutoLowerStringEnum):
    """
    Defines enumerated API names associated with a viewpoint. These names are used to map to specific operations on
    the viewpoint.
    """

    UPDATE = auto()
    DESCRIBE = auto()
    METADATA = auto()
    BOUNDS = auto()
    INFO = auto()
    TILE = auto()
    PREVIEW = auto()
    CROP = auto()
    STATISTICS = auto()


class ViewpointStatus(str, AutoUnderscoreStringEnum):
    """
    Defines status options for a viewpoint. These states represent the state of processing or readiness for a viewpoint.
    """

    NOT_FOUND = auto()
    REQUESTED = auto()
    UPDATING = auto()
    READY = auto()
    DELETED = auto()
    FAILED = auto()


class ViewpointRequest(BaseModel):
    """
    Represents a request to create or update a viewpoint. All fields are required.
    """

    bucket_name: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    viewpoint_name: str = Field(min_length=1)
    tile_size: int = Field(gt=0)
    range_adjustment: RangeAdjustmentType = Field(min_length=1)


class ViewpointModel(BaseModel):
    """
    Represents the model data for a viewpoint, including its ID, name, status, bucket name, object key, tile size,
    range adjustment, local object path, and error message.
    """

    viewpoint_id: str
    viewpoint_name: str
    viewpoint_status: ViewpointStatus
    bucket_name: str
    object_key: str
    tile_size: int
    range_adjustment: RangeAdjustmentType
    local_object_path: str | None
    error_message: str | None
    expire_time: int | None


class ViewpointListResponse(BaseModel):
    """
    Represents the return structure of a request for viewpoints
    """

    items: List[ViewpointModel]
    next_token: Optional[str] = None


class ViewpointUpdate(BaseModel):
    """
    Represents an update operation for a viewpoint. Includes fields to update the viewpoint's ID, name, tile size,
    and range adjustment.
    """

    viewpoint_id: str
    viewpoint_name: str
    tile_size: int
    range_adjustment: RangeAdjustmentType
