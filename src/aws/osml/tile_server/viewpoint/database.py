import json
import logging
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from fastapi import HTTPException

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.viewpoint.models import ViewpointModel, ViewpointStatus


class DecimalEncoder(json.JSONEncoder):
    """
    This is a workaround since DynamoDB returns some of the items as Decimal.
    """

    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return json.JSONEncoder.default(self, obj)


class ViewpointStatusTable:
    def __init__(self, aws_ddb: ServiceResource) -> None:
        self.ddb = aws_ddb
        self.table = self.ddb.Table(ServerConfig.viewpoint_status_table)
        self.logger = logging.getLogger("uvicorn")
        try:
            self.table.table_status
        except ClientError:
            self.logger.error(
                "{} table does not exist in {}.".format(ServerConfig.viewpoint_status_table, ServerConfig.aws_region)
            )
            raise

    def get_all_viewpoints(self) -> List[Dict[str, Any]]:
        """
        Get all the viewpoint items from the dynamodb table

        :returnsList[Dict[str, any]]: list of viewpoints
        """
        try:
            response = self.table.scan()
            return response.get("Items", [])
        except ClientError as err:
            raise HTTPException(
                status_code=err.response["Error"]["Code"],
                detail=f"Cannot fetch an item from ViewpointStatusTable, error: {err.response['Error']['Message']}",
            )
        except KeyError as err:
            raise HTTPException(
                status_code=500,
                detail=f"Invalid Key, it does not exist in ViewpointStatusTable. Please create a new request! {err}",
            )
        except Exception as err:
            raise HTTPException(status_code=500, detail=f"Something went wrong with ViewpointStatusTable! Error: {err}")

    async def get_viewpoint(self, viewpoint_id: str) -> ViewpointModel:
        """
        Get detail of a viewpoint based on a given viewpoint id

        :param viewpoint_id: unique viewpoint id

        :return ViewpointModel: Viewpoint details
        """
        try:
            response = self.table.get_item(Key={"viewpoint_id": viewpoint_id})["Item"]
            return ViewpointModel.model_validate_json(json.dumps(response, cls=DecimalEncoder))
        except ClientError as err:
            raise HTTPException(
                status_code=err.response["Error"]["Code"],
                detail=f"Cannot fetch an item from ViewpointStatusTable, error: {err.response['Error']['Message']}",
            )
        except KeyError as err:
            raise HTTPException(
                status_code=500,
                detail=f"Invalid Key, it does not exist in ViewpointStatusTable. Please create a new request! {err}",
            )
        except Exception as err:
            raise HTTPException(status_code=500, detail=f"Something went wrong with ViewpointStatusTable! Error: {err}")

    def create_viewpoint(self, viewpoint_request: ViewpointModel) -> Dict:
        """
        Create a viewpoint item and store them in a DynamoDB table

        :param viewpoint_request: user given viewpoint details

        :return ViewpointModel: details of a newly created viewpoint
        """
        viewpoint_request_dict = viewpoint_request.model_dump()  # converts to dict

        # TODO remove when implemented a background task
        viewpoint_request_dict["viewpoint_status"] = ViewpointStatus.READY

        try:
            self.table.put_item(Item=viewpoint_request_dict)
            return viewpoint_request_dict
        except ClientError as err:
            raise HTTPException(
                status_code=err.response["Error"]["Code"],
                detail=f"Cannot write to ViewpointStatusTable, error: {err.response['Error']['Message']}",
            )

    def update_viewpoint(self, viewpoint_item: ViewpointModel) -> ViewpointModel:
        """
        Update viewpoint item in a dynamodb table

        :param viewpoint_request: user given viewpoint details

        :return ViewpointModel: details of a updated viewpoint
        """
        self.logger.info(viewpoint_item.model_dump())

        try:
            update_exp, update_attr = self.get_update_params(viewpoint_item.model_dump())

            response = self.table.update_item(
                Key={"viewpoint_id": viewpoint_item.viewpoint_id},
                UpdateExpression=update_exp,
                ExpressionAttributeValues=update_attr,
                ReturnValues="ALL_NEW",
            )

            return ViewpointModel.model_validate_json(json.dumps(response["Attributes"], cls=DecimalEncoder))

        except ClientError as err:
            raise HTTPException(
                status_code=err.response["Error"]["Code"],
                detail=f"Cannot write to ViewpointStatusTable, error: {err.response['Error']['Message']}",
            )
        except Exception as err:
            raise HTTPException(
                status_code=500, detail=f"Something went wrong when updating an item in ViewpointStatusTable! Error: {err}"
            )

    def get_update_params(self, body: Dict) -> Tuple[str, Dict[str, Any]]:
        """
        Generate an update expression and a dict of values to update a dynamodb table.

        :param body: Dict = the body of the request that contains the updated data

        :return: Tuple[str, Dict[str, Any]] = the generated update expression and attributes
        """
        update_expr = ["SET "]
        update_attr = dict()

        for key, val in body.items():
            if key != "viewpoint_id":
                update_expr.append(f" {key} = :{key},")
                update_attr[f":{key}"] = val

        return "".join(update_expr)[:-1], update_attr
