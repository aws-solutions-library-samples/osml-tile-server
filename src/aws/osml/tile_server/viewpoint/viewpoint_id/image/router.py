#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from fastapi import APIRouter, status

from .bounds import bounds_router
from .crop import crop_router
from .info import info_router
from .metadata import metadata_router
from .preview import preview_router
from .statistics import statistics_router
from .tiles import tiles_router

image_router = APIRouter(
    prefix="/image",
    tags=["image"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)

image_router.include_router(metadata_router)
image_router.include_router(bounds_router)
image_router.include_router(info_router)
image_router.include_router(statistics_router)
image_router.include_router(preview_router)
image_router.include_router(tiles_router)
image_router.include_router(crop_router)
