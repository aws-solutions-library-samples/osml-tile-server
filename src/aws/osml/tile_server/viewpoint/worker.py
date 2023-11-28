import asyncio
import json
import logging
from pathlib import Path

from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError

from aws.osml.tile_server.app_config import ServerConfig

from .database import DecimalEncoder, ViewpointStatusTable
from .models import ViewpointModel, ViewpointStatus
from .queue import ViewpointRequestQueue


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
            self.logger.debug("Scanning for SQS messages")
            try:
                messages = self.viewpoint_request_queue.queue.receive_messages(WaitTimeSeconds=5)

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
                        local_viewpoint_folder = Path("/" + ServerConfig.efs_mount_name, message_viewpoint_id)
                        local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
                        local_object_path = Path(local_viewpoint_folder, message_object_key)
                        local_object_path_str = str(local_object_path.absolute())

                        # download file to temp local (TODO update when efs is implemented)
                        retry_count = 0
                        error_message = None
                        while retry_count < 3:
                            try:
                                self.s3.meta.client.download_file(
                                    message_bucket_name, message_object_key, local_object_path_str
                                )

                                viewpoint_new_status = ViewpointStatus.READY
                                self.logger.info(
                                    f"Successfully download to {local_object_path_str}. This Viewpoint is now READY!"
                                )

                                error_message = None

                                break

                            except ClientError as err:
                                if err.response["Error"]["Code"] == "404":
                                    error_message = f"The {message_bucket_name} bucket does not exist! Error={err}"
                                    self.logger.error(error_message)

                                elif err.response["Error"]["Code"] == "403":
                                    error_message = (
                                        f"You do not have permission to access {message_bucket_name} bucket! Error={err}"
                                    )
                                    self.logger.error(error_message)

                                error_message = f"Image Tile Server cannot process your S3 request! Error={err}"
                                self.logger.error(error_message)
                                viewpoint_new_status = ViewpointStatus.FAILED

                                break
                            except Exception as err:
                                error_message = f"Something went wrong! Viewpoint_id: {message_viewpoint_id}! Error={err}"
                                self.logger.error(error_message)
                                viewpoint_new_status = ViewpointStatus.FAILED

                            retry_count += 1

                        # update ddb
                        viewpoint_item.viewpoint_status = viewpoint_new_status
                        viewpoint_item.local_object_path = local_object_path_str

                        if error_message:
                            viewpoint_item.error_message = error_message

                        self.viewpoint_database.update_viewpoint(viewpoint_item)

                        # remove message from the queue since it has been processed
                        message.delete()
                    else:
                        self.logger.error(
                            f"Cannot process {message_viewpoint_id} due to the incorrect "
                            f"Viewpoint Status {message_viewpoint_status}!"
                        )
                        continue

            except ClientError as err:
                self.logger.warning(f"[Worker Background Thread] {err}")
            except KeyError as err:
                self.logger.warning(f"[Worker Background Thread] {err}")
            except Exception as err:
                self.logger.warning(f"[Worker Background Thread] {err}")

            await asyncio.sleep(1)
