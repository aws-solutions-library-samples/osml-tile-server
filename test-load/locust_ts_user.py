import os
import random
import time
from math import ceil, log
from pathlib import Path
from secrets import token_hex
from typing import List, Optional

import boto3
import gevent
from botocore.config import Config
from hilbertcurve.hilbertcurve import HilbertCurve
from locust import FastHttpUser, between, events, task

VIEWPOINT_STATUS = "viewpoint_status"

VIEWPOINT_ID = "viewpoint_id"


@events.init_command_line_parser.add_listener
def _(parser):
    """
    This method does XYZ.

    :param parser: The parser object used for parsing.
    :return: The result of the method.
    """
    parser.add_argument(
        "--test-images-bucket",
        type=str,
        env_var="LOCUST_TEST_IMAGES_BUCKET",
        default="rawimagery-parrised-devaccount",
        help="Name of bucket containing test imagery",
    )
    parser.add_argument(
        "--test-images-prefix",
        type=str,
        env_var="LOCUST_TEST_IMAGES_PREFIX",
        default="",
        help="Prefix of object keys to test",
    )


@events.test_start.add_listener
def _(environment, **kwargs):
    """
    This method prints the test images bucket and object prefix from the given environment.

    :param environment: The environment object containing parsed options.
    :param kwargs: Additional keyword arguments (unused).
    :return: None
    """
    print(f"Using imagery from: {environment.parsed_options.test_images_bucket}")
    print(f"With object Prefix: {environment.parsed_options.test_images_prefix}")


class TileServerUser(FastHttpUser):
    """
    :class:`TileServerUser` is a class representing a user that interacts with a tile server. It inherits from
    `FastHttpUser` class provided by the `locust` library. The class provides methods for simulating user behavior on
    the tile server, such as creating, retrieving, and discarding viewpoints, as well as querying metadata, bounds,
    info, and statistics of existing viewpoints.

    Examples:
        Creating an instance of :class:`TileServerUser` and running a load test with Locust:

        .. code-block:: python

            from locust import User, TaskSet, constant, HttpUser, between
            from tile.server.user import TileServerUser

            class MyUser(HttpUser):
                tasks = [TileServerUser]
                wait_time = between(1, 2)

    To run the locust file:
    $res locust -f filename.py with python 3.8.5 and above; for old versions of python we may use locustio instead of locust
    :class:`TileServerUser` provides the following instance variables:
        - `test_images_bucket`: The S3 bucket name for test images.
        - `test_images_prefix`: The prefix for filtering test images within the S3 bucket.
        - `test_image_keys`: The list of test image keys in the S3 bucket.
        - `wait_time`: The time interval (in seconds) between each task execution.

    """

    # Establishes a 1-2 second wait between tasks
    wait_time = between(1, 2)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_images_bucket = self.environment.parsed_options.test_images_bucket
        self.test_images_prefix = self.environment.parsed_options.test_images_prefix or None
        self.test_image_keys = []

    def on_start(self) -> None:
        """
        Locust invokes this method when the user is created. It identifies the set of test imagery object keys
        from the S3 bucket provided.
        """
        s3_client = boto3.client("s3", config=Config(region_name=os.getenv("AWS_DEFAULT_REGION", "us-west-2")))
        paginator = s3_client.get_paginator("list_objects_v2")
        response_iterator = paginator.paginate(
            Bucket=self.test_images_bucket,
            Prefix=self.test_images_prefix,
            PaginationConfig={"MaxItems": 200, "PageSize": 20},
        )
        for response in response_iterator:
            if "Contents" not in response:
                print(response)

            for item in response["Contents"]:
                object_key = item["Key"]
                object_suffix = Path(object_key).suffix.lower()
                if object_suffix in [".tif", ".tiff", ".ntf", ".nitf"]:
                    self.test_image_keys.append(object_key)

        if not self.test_image_keys:
            raise ValueError(f"Unable to find any test imagery in {self.test_images_bucket}")
        else:
            print(f"Found {len(self.test_image_keys)} test images in {self.test_images_bucket}")

    @task(5)
    def view_new_image_behavior(self) -> None:
        """
        This task simulates a user creating, retrieving tiles from, and then discarding a viewpoint.
        """
        viewpoint_id = self.create_viewpoint(self.test_images_bucket, random.choice(self.test_image_keys))
        if viewpoint_id is not None:
            final_status = self.wait_for_viewpoint_ready(viewpoint_id)
            if final_status == "READY":
                self.request_tiles(viewpoint_id)

            if final_status not in ["DELETED", "NOT_FOUND"]:
                self.cleanup_viewpoint(viewpoint_id)

    @task(1)
    def discover_viewpoints_behavior(self) -> None:
        """
        This task simulates a user accessing a web page that displays an active list of viewpoints. The main query
        API is invoked then details including the image preview, metadata, and detailed statistics are called
        for each image.
        """

        # TODO: Update this to work on a per-page basis once the list viewpoints operation is paginated
        viewpoint_ids = self.list_ready_viewpoints()

        def get_viewpoint_details(viewpoint_id: str):
            self.get_viewpoint_metadata(viewpoint_id)
            self.get_viewpoint_info(viewpoint_id)
            self.get_viewpoint_bounds(viewpoint_id)
            # self.get_viewpoint_preview(viewpoint_id)  # TODO: Reenable Preview once it isn't super slow
            self.get_viewpoint_statistics(viewpoint_id)

        pool = gevent.pool.Pool()
        for viewpoint_id in viewpoint_ids:
            pool.spawn(get_viewpoint_details, viewpoint_id)
        pool.join()

    def create_viewpoint(self, test_images_bucket: str, test_image_key: str) -> Optional[str]:
        """
        Creates a viewpoint with specified parameters.

        :param test_images_bucket: bucket containing test images
        :param test_image_key: key of the test image
        :return: ID of the created viewpoint or None
        """
        with self.rest(
            "POST",
            "/viewpoints",
            name="CreateViewpoint",
            json={
                "viewpoint_name": "LocustUser-Viewpoint-" + token_hex(16),
                "bucket_name": test_images_bucket,
                "object_key": test_image_key,
                "tile_size": random.choice([256, 512]),
                "range_adjustment": random.choice(["NONE", "DRA", "MINMAX"]),
            },
        ) as response:
            if response.js is not None:
                if VIEWPOINT_ID not in response.js:
                    response.failure(f"'{VIEWPOINT_ID}' missing from response {response.text}")
                else:
                    return response.js[VIEWPOINT_ID]
        return None

    def wait_for_viewpoint_ready(self, viewpoint_id: str) -> str:
        """
        Waits for the viewpoint with specified ID to become ready.

        :param viewpoint_id: ID of the viewpoint to wait for
        :return: final status of the viewpoint
        """
        done = False
        num_retries = 120
        final_status = "NOT_FOUND"
        while not done and num_retries > 0:
            with self.rest("GET", f"/viewpoints/{viewpoint_id}", name="DescribeViewpoint") as response:
                if response.js is not None and VIEWPOINT_STATUS in response.js:
                    final_status = response.js[VIEWPOINT_STATUS]
                    if response.js[VIEWPOINT_STATUS] in ["READY", "FAILED", "DELETED"]:
                        done = True
                    else:
                        time.sleep(5)
                        num_retries -= 1
        if not done:
            response.failure(f"Gave up waiting for {viewpoint_id} to become ready. Final Status was {final_status}")

        return final_status

    def request_tiles(self, viewpoint_id: str, num_tiles: int = 100, batch_size: int = 5) -> None:
        """
        Requests tiles for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to request tiles for
        :param num_tiles: number of tiles to request
        :param batch_size: number of tiles to request in parallel
        :return: None
        """
        tile_format = "PNG"
        compression = "NONE"

        def concurrent_tile_request(tile: (int, int, int)):
            url = f"/viewpoints/{viewpoint_id}/tiles/{tile[2]}/{tile[0]}/{tile[1]}.{tile_format}?compression={compression}"
            with self.client.get(url, name="GetTile") as response:
                if not response.content:
                    response.failure("GetTile response contained no content")

        for z in [3, 2, 1, 0]:
            num_tiles_at_zoom = ceil(num_tiles / (4**z)) 
            p = ceil(log(num_tiles_at_zoom) / (2 * log(2)))
            n = 2
            hilbert_curve = HilbertCurve(p, n)
            for i in range(0, num_tiles_at_zoom, batch_size):
                distances = list(range(i, min(i + batch_size, num_tiles_at_zoom)))
                tiles = [(p[0], p[1], z) for p in hilbert_curve.points_from_distances(distances)]
                pool = gevent.pool.Pool()
                for tile in tiles:
                    pool.spawn(concurrent_tile_request, tile)
                pool.join()

    def cleanup_viewpoint(self, viewpoint_id: str) -> None:
        """
        Deletes the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to delete
        """
        with self.rest("DELETE", f"/viewpoints/{viewpoint_id}", name="DeleteViewpoint") as response:
            if response.js is not None:
                if VIEWPOINT_STATUS not in response.js:
                    response.failure(f"'{VIEWPOINT_STATUS}' missing from response {response.text}")
                elif response.js[VIEWPOINT_STATUS] != "DELETED":
                    response.failure(f"Unexpected status after viewpoint delete {response.text}")

    def list_ready_viewpoints(self) -> List[str]:
        """
        Lists all ready viewpoints.

        :return: list of viewpoint IDs
        """
        result = []
        with self.rest("GET", "/viewpoints", name="ListViewpoints") as response:
            if response.js is not None:
                for viewpoint in response.js:
                    if (
                        VIEWPOINT_ID in viewpoint
                        and VIEWPOINT_STATUS in viewpoint
                        and viewpoint[VIEWPOINT_STATUS] == "READY"
                    ):
                        result.append(viewpoint[VIEWPOINT_ID])
        return result

    def get_viewpoint_metadata(self, viewpoint_id: str):
        """
        Fetches metadata for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to fetch metadata for
        """
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/metadata", name="GetMetadata") as response:
            if response.js is not None and "metadata" not in response.js:
                response.failure(f"'metadata' missing from response {response.text}")

    def get_viewpoint_bounds(self, viewpoint_id: str):
        """
        Fetches bounds for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to fetch bounds for
        """
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/bounds", name="GetBounds") as response:
            if response.js is not None and "bounds" not in response.js:
                response.failure(f"'bounds' missing from response {response.text}")

    def get_viewpoint_info(self, viewpoint_id: str):
        """
        Fetches info for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to fetch info for
        """
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/info", name="GetInfo") as response:
            if response.js is not None and "features" not in response.js:
                response.failure(f"'features' missing from response {response.text}")

    def get_viewpoint_statistics(self, viewpoint_id: str):
        """
        Fetches statistics for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to fetch statistics for
        """
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/statistics", name="GetStatistics") as response:
            if response.js is not None and "image_statistics" not in response.js:
                response.failure(f"'image_statistics' missing from response {response.text}")

    def get_viewpoint_preview(self, viewpoint_id: str):
        """
        Fetches preview for the viewpoint with specified ID.

        :param viewpoint_id: ID of the viewpoint to fetch preview for
        """
        tile_format = "PNG"
        with self.client.get(f"/viewpoints/{viewpoint_id}/preview.{tile_format}", name="GetPreview") as response:
            if not response.content:
                response.failure("GetPreview response contained no content")
