import json
import logging
import os
from pathlib import Path
from time import sleep
import asyncio

from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError

from .database import DecimalEncoder, ViewpointStatusTable
from .models import ViewpointModel, ViewpointStatus
from .queue import ViewpointRequestQueue

FILESYSTEM_CACHE_ROOT = os.getenv("VIEWPOINT_FILESYSTEM_CACHE", "/tmp/viewpoint")


class ViewpointWorker:
    def __init__(
        self,
        viewpoint_request_queue: ViewpointRequestQueue,
        aws_s3: ServiceResource,
        viewpoint_database: ViewpointStatusTable,
    ):
        self.viewpoint_request_queue = viewpoint_request_queue
        self.s3 = aws_s3
        self.viewpoint_database = viewpoint_database
        self.logger = logging.getLogger("uvicorn")

    async def run(self) -> None:
        """
        Monitors SQS queues for ViewpointRequest and be able to process it. First, it will
        pick up a message from ViewpointRequest SQS. Then, it will download an image from S3
        and save it to the local temp directory. Once that's completed, it will update the DynamoDB
        to reflect that this Viewpoint is READY to review. This function will run in the background.

        :return: None
        """
        while True:
            self.logger.info("Scanning for SQS messages")
            try:
                messages = self.viewpoint_request_queue.queue.receive_messages()
                if len(messages) == 0:
                    await asyncio.sleep(5)  # sleep for 5 seconds and retry
                for message in messages:
                    self.logger.info(f"MESSAGE: {message.body}")
                    message_attributes = json.loads(message.body)
                    viewpoint_item = ViewpointModel.model_validate_json(json.dumps(message_attributes, cls=DecimalEncoder))

                    viewpoint_new_status = ViewpointStatus.REQUESTED

                    message_viewpoint_id = viewpoint_item.viewpoint_id
                    message_viewpoint_status = viewpoint_item.viewpoint_status
                    message_object_key = viewpoint_item.object_key
                    message_bucket_name = viewpoint_item.bucket_name

                    if message_viewpoint_status == ViewpointStatus.REQUESTED:
                        # create a temp file
                        local_viewpoint_folder = Path(FILESYSTEM_CACHE_ROOT, message_viewpoint_id)
                        local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
                        local_object_path = Path(local_viewpoint_folder, message_object_key)

                        # download file to temp local (TODO update when efs is implemented)
                        retry_count = 0
                        internal_err = None
                        while retry_count < 3:
                            try:
                                self.s3.meta.client.download_file(
                                    message_bucket_name, message_object_key, str(local_object_path.absolute())
                                )

                                viewpoint_new_status = ViewpointStatus.READY
                                self.logger.info(
                                    f"Successfully download to {str(local_object_path.absolute())}. This Viewpoint is now READY!"
                                )

                                internal_err = None

                                break

                            except ClientError as err:
                                if err.response["Error"]["Code"] == "404":
                                    internal_err = f"The {message_bucket_name} bucket does not exist! Error={err}"
                                    self.logger.error(internal_err)

                                elif err.response["Error"]["Code"] == "403":
                                    internal_err = (
                                        f"You do not have permission to access {message_bucket_name} bucket! Error={err}"
                                    )
                                    self.logger.error(internal_err)

                                internal_err = f"Image Tile Server cannot process your S3 request! Error={err}"
                                self.logger.error(internal_err)
                                viewpoint_new_status = ViewpointStatus.FAILED
                            except Exception as err:
                                internal_err = f"Something went wrong! Viewpoint_id: {message_viewpoint_id}! Error={err}"
                                self.logger.error(internal_err)
                                viewpoint_new_status = ViewpointStatus.FAILED

                            retry_count += 1

                        # update ddb
                        viewpoint_item.viewpoint_status = viewpoint_new_status
                        viewpoint_item.local_object_path = str(local_object_path.absolute())

                        if internal_err:
                            viewpoint_item.internal_err = internal_err

                        self.viewpoint_database.update_viewpoint(viewpoint_item)

                        # remove message from the queue since it has been processed
                        message.delete()
                    else:
                        self.logger.error(
                            f"Cannot process {message_viewpoint_id} due to the incorrect Viewpoint Status {message_viewpoint_status}!"
                        )
                        continue

            except ClientError as err:
                self.logger.warning(f"[Worker Background Thread] {err}")
            except KeyError as err:
                self.logger.warning(f"[Worker Background Thread] {err}")
            except Exception as err:
                self.logger.warning(f"[Worker Background Thread] {err}")

            await asyncio.sleep(5)
