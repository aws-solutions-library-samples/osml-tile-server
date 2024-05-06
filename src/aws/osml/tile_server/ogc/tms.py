#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Optional

from pydantic import BaseModel, Field

from .common_core import Link
from .common_geodata import DataType


class BoundingBox2D(BaseModel):
    """
    A minimum bounding rectangle surrounding a 2D resource.
    """

    lower_left: tuple[float, float] = Field(serialization_alias="lowerLeft")
    upper_right: tuple[float, float] = Field(serialization_alias="upperRight")
    crs: Optional[str] = Field(default=None)


class TilePoint(BaseModel):
    """
    A specific tile location within a tileset.
    """

    coordinates: tuple[float, float] = Field(description="The coordinate of the 2D location.")
    crs: Optional[str] = Field(description="Coordinate Reference System (CRS) of the coordinates", default=None)
    tile_matrix: Optional[str] = Field(serialization_alias="tileMatrix", default=None)


class TileMatrixLimits(BaseModel):
    """
    Limits for the tileRow and tileCol values within a tileMatrix in a tileset. This defines the boundary of a
    2D resource within a given tileset resolution level.
    """

    tile_matrix: str = Field(serialization_alias="tileMatrix")
    min_tile_row: int = Field(serialization_alias="minTileRow", ge=0)
    max_tile_row: int = Field(serialization_alias="maxTileRow", ge=0)
    min_tile_col: int = Field(serialization_alias="minTileCol", ge=0)
    max_tile_col: int = Field(serialization_alias="maxTileCol", ge=0)


class TileSetItem(BaseModel):
    """
    A minimal tileset element for use within a list of tilesets linking to full description of those tilesets.
    """

    title: Optional[str] = Field(description="A title for this tileset.", default=None)
    data_type: DataType = Field(serialization_alias="dataType", description="Type of data represented in the tileset")
    links: list[Link] = Field(description="Links to related resources.")
    crs: str = Field()
    tile_matrix_set_uri: Optional[str] = Field(
        serialization_alias="tileMatrixSetURI",
        description="Reference to a Tile Matrix Set on an official source for Tile Matrix Sets such as the OGC NA"
        " definition server. Required if the tile matrix set is registered on an open official source.",
        default=None,
    )


class TileSetList(BaseModel):
    tilesets: list[TileSetItem] = Field()
    links: Optional[list[Link]] = Field(description="Links to related resources.", default=None)


class TileSetMetadata(BaseModel):
    title: Optional[str] = Field(description="A title for this tileset.", default=None)
    description: Optional[str] = Field(description="Brief narrative description of this tile set", default=None)
    data_type: DataType = Field(serialization_alias="dataType", description="Type of data represented in the tileset")
    crs: str = Field()
    tile_matrix_set_uri: Optional[str] = Field(
        serialization_alias="tileMatrixSetURI",
        description="Reference to a Tile Matrix Set on an official source for Tile Matrix Sets such as the OGC NA"
        " definition server. Required if the tile matrix set is registered on an open official source.",
        default=None,
    )
    links: list[Link] = Field(description="Links to related resources.")
    tile_matrix_set_limits: Optional[list[TileMatrixLimits]] = Field(
        serialization_alias="tileMatrixSetLimits",
        description="Limits for the TileRow and TileCol values for each TileMatrix in the TileMatrixSet. "
        "If missing, there are no limits other than the ones imposed by the TileMatrixSet.",
        default=None,
    )
    epoch: Optional[float] = Field(description="Epoch of the Coordinate Reference System (CRS)", default=None)
    bounding_box: Optional[BoundingBox2D] = Field(
        serialization_alias="boundingBox",
        description="Minimum bounding rectangle surrounding the tile matrix set, in the supported CRS",
        default=None,
    )
    center_point: Optional[TilePoint] = Field(
        serialization_alias="centerPoint", description="Location of a tile that nicely represents the tileset.", default=None
    )
