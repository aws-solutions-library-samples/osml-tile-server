import json
import logging
import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[Any, Any], context: Any):
    # initialize ddb service
    ts_status_table = os.getenv("JOB_TABLE", "TSJobTable")
    ddb = boto3.resource("dynamodb")
    ddb_table = ddb.Table(ts_status_table)

    logger.info(f"Received an event from SQS: {event}")

    try:
        for record in event["Records"]:
            message_body = json.loads(record["body"])
            if message_body:
                viewpoint_id = message_body["viewpoint_id"]

                # get item from ddb
                item = ddb_table.get_item(Key={"viewpoint_id": viewpoint_id})

                if item:
                    # set the item's status to FAILED and add expire_time
                    viewpoint_status = "FAILED"
                    time_now = datetime.utcnow()
                    expire_time = time_now + timedelta(1)

                    # update the item in the ddb
                    response = ddb_table.update_item(
                        Key={"viewpoint_id": viewpoint_id},
                        UpdateExpression="set viewpoint_status=:viewpoint_status, expire_time=:expire_time",
                        ExpressionAttributeValues={
                            ":viewpoint_status": viewpoint_status,
                            ":expire_time": int(expire_time.timestamp()),
                        },
                        ReturnValues="ALL_NEW",
                    )

                    logger.info(f"Update completed! {response}")

                    # return the result and the message should be removed from the DLQ
                    return {"statusCode": 200, "body": response}
                else:
                    return {"statusCode": 404, "body": f"There's no viewpoint_id {viewpoint_id} found in DynamoDb!"}
            else:
                return {"statusCode": 404, "body": "There's no viewpoint_message found in DLQ!"}
    except Exception:
        logger.error(traceback.format_exc())
        return {"statusCode": 500, "body": f"Internal Service Error! {traceback.format_exc()}"}
