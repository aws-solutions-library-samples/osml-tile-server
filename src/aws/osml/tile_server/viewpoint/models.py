import logging
from enum import Enum

from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn")


class PixelRangeAdjustmentType(str, Enum):
    NONE = "NONE"
    MINMAX = "MINMAX"
    DRA = "DRA"


class ViewpointApiNames(str, Enum):
    UPDATE = "update"
    DESCRIBE = "describe"
    METADATA = "metadata"
    BOUNDS = "bounds"
    INFO = "info"
    TILE = "tile"
    PREVIEW = "preivew"
    CROP = "crop"
    STATISTICS = "statistics"


class ViewpointStatus(str, Enum):
    NOT_FOUND = "NOT FOUND"
    REQUESTED = "REQUESTED"
    UPDATING = "UPDATING"
    READY = "READY"
    DELETED = "DELETED"
    FAILED = "FAILED"


class ViewpointRequest(BaseModel):
    bucket_name: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    viewpoint_name: str = Field(min_length=1)
    tile_size: int = Field(gt=0)
    range_adjustment: PixelRangeAdjustmentType = Field(min_length=1)


class ViewpointModel(BaseModel):
    viewpoint_id: str
    viewpoint_name: str
    viewpoint_status: ViewpointStatus
    bucket_name: str
    object_key: str
    tile_size: int
    range_adjustment: PixelRangeAdjustmentType
    local_object_path: str | None


class ViewpointUpdate(BaseModel):
    viewpoint_id: str
    viewpoint_name: str
    tile_size: int
    range_adjustment: PixelRangeAdjustmentType
