#  Copyright 2024 Amazon.com, Inc. or its affiliates.
from enum import Enum


class DataType(str, Enum):
    """
    Common data types defined by the OGC APIs. Note this list may be extended in the future (e.g. point clouds, meshes,
    etc.).
    """

    MAP = "map"
    VECTOR = "vector"
    COVERAGE = "coverage"
