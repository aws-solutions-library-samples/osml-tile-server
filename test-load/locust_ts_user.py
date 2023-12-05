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
    print(f"Using imagery from: {environment.parsed_options.test_images_bucket}")
    print(f"With object Prefix: {environment.parsed_options.test_images_prefix}")


class TileServerUser(FastHttpUser):
    # Establishes a 1-2 second wait between tasks
    wait_time = between(1, 2)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_images_bucket = self.environment.parsed_options.test_images_bucket
        self.test_images_prefix = self.environment.parsed_options.test_images_prefix or None
        self.test_image_keys = []

    def on_start(self):
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
        done = False
        num_retries = 30
        final_status = "NOT_FOUND"
        while not done and num_retries > 0:
            with self.rest("GET", f"/viewpoints/{viewpoint_id}", name="DescribeViewpoint") as response:
                if response.js is not None and VIEWPOINT_STATUS in response.js:
                    final_status = response.js[VIEWPOINT_STATUS]
                    if response.js[VIEWPOINT_STATUS] in ["READY", "FAILED", "DELETED"]:
                        done = True
                    else:
                        time.sleep(2)
        return final_status

    def request_tiles(self, viewpoint_id: str, num_tiles: int = 100, batch_size: int = 5):
        z = 0
        tile_format = "PNG"
        compression = "NONE"

        def concurrent_tile_request(tile: [int, int]):
            url = f"/viewpoints/{viewpoint_id}/tiles/{z}/{tile[0]}/{tile[1]}.{tile_format}?compression={compression}"
            with self.client.get(url, name="GetTile") as response:
                if not response.content:
                    response.failure("GetTile response contained no content")

        p = ceil(log(num_tiles) / (2 * log(2)))
        n = 2
        hilbert_curve = HilbertCurve(p, n)
        for i in range(0, num_tiles, batch_size):
            distances = list(range(i, i + batch_size))
            tiles = hilbert_curve.points_from_distances(distances)
            pool = gevent.pool.Pool()
            for tile in tiles:
                pool.spawn(concurrent_tile_request, tile)
            pool.join()

    def cleanup_viewpoint(self, viewpoint_id: str):
        with self.rest("DELETE", f"/viewpoints/{viewpoint_id}", name="DeleteViewpoint") as response:
            if response.js is not None:
                if VIEWPOINT_STATUS not in response.js:
                    response.failure(f"'{VIEWPOINT_STATUS}' missing from response {response.text}")
                elif response.js[VIEWPOINT_STATUS] != "DELETED":
                    response.failure(f"Unexpected status after viewpoint delete {response.text}")

    def list_ready_viewpoints(self) -> List[str]:
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
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/metadata", name="GetMetadata") as response:
            if response.js is not None and "metadata" not in response.js:
                response.failure(f"'metadata' missing from response {response.text}")

    def get_viewpoint_bounds(self, viewpoint_id: str):
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/bounds", name="GetBounds") as response:
            if response.js is not None and "bounds" not in response.js:
                response.failure(f"'bounds' missing from response {response.text}")

    def get_viewpoint_info(self, viewpoint_id: str):
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/info", name="GetInfo") as response:
            if response.js is not None and "features" not in response.js:
                response.failure(f"'features' missing from response {response.text}")

    def get_viewpoint_statistics(self, viewpoint_id: str):
        with self.rest("GET", f"/viewpoints/{viewpoint_id}/statistics", name="GetStatistics") as response:
            if response.js is not None and "image_statistics" not in response.js:
                response.failure(f"'image_statistics' missing from response {response.text}")

    def get_viewpoint_preview(self, viewpoint_id: str):
        tile_format = "PNG"
        with self.client.get(f"/viewpoints/{viewpoint_id}/preview.{tile_format}", name="GetPreview") as response:
            if not response.content:
                response.failure("GetPreview response contained no content")
