import json
import logging
from decimal import Decimal
from typing import Any, Dict, Tuple

from boto3.dynamodb.conditions import Attr
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from fastapi import HTTPException

from aws.osml.tile_server.app_config import ServerConfig

from .models import ViewpointModel


class DecimalEncoder(json.JSONEncoder):
    """
    This is a helper class which extends json.JSONEncoder.
    It's used to convert all Decimal instances to int
    when we fetch data from DynamoDB (which can return some items as Decimal).

    :param json.JSONEncoder: Inherited JSON Encoder class from json module
    :type json.JSONEncoder: class:`json.JSONEncoder`
    """

    def default(self, obj):
        """
        Overriden default method from JSONEncoder class.
        Converts all Decimal instances to int else returns default conversion.

        :param obj: Object for JSON Encoding
        :type obj: Object
        :return: Integer representation of Decimal else default JSON Conversion
        :rtype: `int` or as defined by `json.JSONEncoder`
        """
        if isinstance(obj, Decimal):
            return int(obj)
        return super().default(obj)


class ViewpointStatusTable:
    """
    A class used to represent the ViewpointStatusTable.

    :param aws_ddb: An instance of a service resource of Boto3's DynamoDB
    """

    def __init__(self, aws_ddb: ServiceResource) -> None:
        """
        Initialize the ViewpointStatusTable and validate the table status.

        :raise Exception: If the table does not exist in the given region.
        """
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

    def get_viewpoints(self, limit: int = None, next_token: str = None) -> Dict[str, Any]:
        """
        Get viewpoint items from the dynamodb table.  If limit nor next_token are provided it returns all records.

        :param limit: Optional. max number of viewpoints requested from dynamodb
        :param next_token: Optional. the token to begin a query from.  provided by the previous query response that
                had more records available
        :returns Dict[str, any]: {"Items": [list of viewpoints], <"nextToken">: string token to query for more results}
        """
        query_params = {"FilterExpression": Attr("viewpoint_status").ne("DELETED")}
        if limit:
            query_params["Limit"] = limit
        if next_token:
            query_params["ExclusiveStartKey"] = {"viewpoint_id": next_token}
        try:
            if query_params:
                return self.get_paged_viewpoints(query_params)
            return self.get_all_viewpoints()
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

    def get_all_viewpoints(self) -> Dict[str, Any]:
        """
        Get all the viewpoint items from the dynamodb table

        :returns Dict[str, any]: {"Items": [list of viewpoints]}
        """
        response = self.table.scan()
        data = response["Items"]
        while response.get("LastEvaluatedKey"):
            response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            data.extend(response["Items"])
        return {"Items": data}

    def get_paged_viewpoints(self, query_params: Dict) -> Dict[str, Any]:
        """
        Get a page of viewpoint items from the dynamodb table

        :returns Dict[str, any]: {"Items": [list of viewpoints], <"nextToken">: string token to query for more results}
        """
        response = self.table.scan(**query_params)
        ret_val = {"Items": response["Items"]}
        if response.get("LastEvaluatedKey"):
            ret_val["nextToken"] = response["LastEvaluatedKey"]["viewpoint_id"]
        return ret_val

    def get_viewpoint(self, viewpoint_id: str) -> ViewpointModel:
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
