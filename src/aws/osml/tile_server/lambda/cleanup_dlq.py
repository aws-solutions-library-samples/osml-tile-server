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
    """
    This Lambda function processes SQS queue messages, retrieving and updating the associated items from a DynamoDB
    table. It expects an event object with a "Records" list which includes a 'viewpoint_id'. If viewpoint_id is missing
    or item is not found in the table, it returns a 404 response. If the item is found, its status is updated as
    'FAILED' with an expiry time of current time plus 1 day. If an exception occurs, it's logged and a 500-status code
    is returned with the error message.

    Example usage:
        event = {
            "Records": [
                {
                    "body": "{\"viewpoint_id\": \"123\"}"
                }
            ]
        }
        context = {}
        result = lambda_handler(event, context)

        print(result)

    :param event: The event object that triggered the Lambda function. It contains information about the event source
        and any data associated with the event.
    :param context: The runtime information of the Lambda function. It provides methods and properties that allow you to
        interact with the runtime environment.
    :return: A dictionary containing the response status code and body.
    """

    # Initialize ddb service
    ts_status_table = os.getenv("JOB_TABLE", "TSJobTable")
    ddb = boto3.resource("dynamodb")
    ddb_table = ddb.Table(ts_status_table)

    logger.info(f"Received an event from SQS: {event}")

    try:
        for record in event["Records"]:
            message_body = json.loads(record["body"])
            if message_body:
                viewpoint_id = message_body["viewpoint_id"]

                # Get item from ddb
                item = ddb_table.get_item(Key={"viewpoint_id": viewpoint_id})

                if item:
                    # Set the item's status to FAILED and add expire_time
                    viewpoint_status = "FAILED"
                    time_now = datetime.utcnow()
                    expire_time = time_now + timedelta(1)

                    # Update the item in the ddb
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

                    # Return the result and the message should be removed from the DLQ
                    return {"statusCode": 200, "body": response}
                else:
                    return {"statusCode": 404, "body": f"There's no viewpoint_id {viewpoint_id} found in DynamoDb!"}
            else:
                return {"statusCode": 404, "body": "There's no viewpoint_message found in DLQ!"}
    except Exception as err:
        logger.error(err.with_traceback)
        return {"statusCode": 500, "body": f"Internal Service Error! {traceback.format_exc()}"}
