import logging
from enum import auto
from typing import List, Optional

from pydantic import BaseModel, Field

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.utils import AutoLowerStringEnum, AutoUnderscoreStringEnum

logger = logging.getLogger("uvicorn")


class ViewpointApiNames(str, AutoLowerStringEnum):
    """
    Provides enumeration of API names associated with a viewpoint.

    These API names are used to map to specific operations on the viewpoint.

    :cvar UPDATE: Update viewpoint.
    :cvar DESCRIBE: Describe viewpoint.
    :cvar METADATA: Viewpoint metadata.
    :cvar BOUNDS: Viewpoint bounds.
    :cvar INFO: Viewpoint information.
    :cvar TILE: Viewpoint tile.
    :cvar PREVIEW: Viewpoint preview.
    :cvar CROP: Crop viewpoint.
    :cvar STATISTICS: Viewpoint statistics.
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
    Provides status options for a viewpoint.

    These states represent the state of processing or readiness for a viewpoint.

    :cvar NOT_FOUND: Viewpoint isn't found.
    :cvar REQUESTED: Viewpoint requested.
    :cvar UPDATING: Viewpoint updating.
    :cvar READY: Viewpoint ready.
    :cvar DELETED: Viewpoint deleted.
    :cvar FAILED: Viewpoint failed.
    """

    NOT_FOUND = auto()
    REQUESTED = auto()
    UPDATING = auto()
    READY = auto()
    DELETED = auto()
    FAILED = auto()


class ViewpointRequest(BaseModel):
    """
    Represents a request to create or update a viewpoint.

    All fields are required.

    :param bucket_name: The name of the bucket, the Minimum length is 1.
    :param object_key: The key of the object, the Minimum length is 1.
    :param viewpoint_name: The name of the viewpoint, the Minimum length is 1.
    :param tile_size: The tile size, it Should be greater than 0.
    :param range_adjustment: The range adjustment type, the Minimum string length is 1.
    """

    viewpoint_name: str = Field(min_length=1)
    bucket_name: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    tile_size: int = Field(gt=0)
    range_adjustment: RangeAdjustmentType = Field(min_length=1)


class ViewpointModel(BaseModel):
    """
    Represents the model data for a viewpoint, including its ID, name, status, bucket name, object key, tile size,
    range adjustment, local object path, and error message.

    :param viewpoint_id: The ID to associate with the viewpoint.
    :param viewpoint_name: The name of the viewpoint.
    :param viewpoint_status: The status of the viewpoint.
    :param bucket_name: The name of the bucket the images are located in.
    :param object_key: The object is key to associate with the viewpoint.
    :param tile_size: The tile size to use for the viewpoint model.
    :param range_adjustment: The type of range adjustment.
    :param local_object_path: Optional local path of the object.
    :param error_message: Optional error messages to include.
    :param expire_time: Optional expiration time for the message.
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
    Represents an update operation for a viewpoint.

    This includes fields to update the viewpoint's ID, name, tile size, and range adjustment.

    :param viewpoint_id: The ID of the viewpoint.
    :param viewpoint_name: The name of the viewpoint.
    :param tile_size: The tile size.
    :param range_adjustment: The type of range adjustment.
    """

    viewpoint_id: str
    viewpoint_name: str
    tile_size: int
    range_adjustment: RangeAdjustmentType
