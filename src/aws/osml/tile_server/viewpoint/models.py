import logging
from enum import auto

from pydantic import BaseModel, Field

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.utils import AutoLowerStringEnum, AutoUnderscoreStringEnum

logger = logging.getLogger("uvicorn")


class ViewpointApiNames(str, AutoLowerStringEnum):
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
    NOT_FOUND = auto()
    REQUESTED = auto()
    UPDATING = auto()
    READY = auto()
    DELETED = auto()
    FAILED = auto()


class ViewpointRequest(BaseModel):
    bucket_name: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    viewpoint_name: str = Field(min_length=1)
    tile_size: int = Field(gt=0)
    range_adjustment: RangeAdjustmentType = Field(min_length=1)


class ViewpointModel(BaseModel):
    viewpoint_id: str
    viewpoint_name: str
    viewpoint_status: ViewpointStatus
    bucket_name: str
    object_key: str
    tile_size: int
    range_adjustment: RangeAdjustmentType
    local_object_path: str | None
    error_message: str | None


class ViewpointUpdate(BaseModel):
    viewpoint_id: str
    viewpoint_name: str
    tile_size: int
    range_adjustment: RangeAdjustmentType
