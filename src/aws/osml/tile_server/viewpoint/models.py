from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class PixelRangeAdjustmentType(str, Enum):
    NONE = "NONE"
    MINMAX = "MINMAX"
    DRA = "DRA"


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

class UpdateViewpointRequest(BaseModel):
    viewpoint_name: str = Field(min_length=1)
    tile_size: int = Field(gt=0)
    range_adjustment: PixelRangeAdjustmentType = Field(min_length=1)

class ViewpointDescription(BaseModel):
    viewpoint_id: str
    viewpoint_name: str
    bucket_name: str
    object_key: str
    status: ViewpointStatus
    tile_size: int
    range_adjustment: PixelRangeAdjustmentType
    local_object_path: Path


class ViewpointSummary(BaseModel):
    viewpoint_id: str
    status: ViewpointStatus

    @classmethod
    def from_description(cls, description: ViewpointDescription) -> "ViewpointSummary":
        return cls(viewpoint_id=description.viewpoint_id, status=description.status)


class InternalViewpointState(BaseModel):
    description: ViewpointDescription
    local_object_path: Path
