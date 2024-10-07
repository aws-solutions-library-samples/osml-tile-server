#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
from decimal import Decimal
from logging import Logger, getLogger
from typing import Any, Dict, Tuple

from boto3.dynamodb.conditions import Attr
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from fastapi import HTTPException

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.models import ViewpointListResponse, ViewpointModel


class DecimalEncoder(json.JSONEncoder):
    """
    This is a helper class that extends json.JSONEncoder.
    It's used to convert all Decimal instances to int
    when we fetch data from DynamoDB (which can return some items as Decimal).

    :param json.JSONEncoder: Inherited JSON Encoder class from json module.
    """

    def default(self, obj):
        """
        Overriden default method from JSONEncoder class.
        Converts all Decimal instances to int else returns default conversion.

        :param obj: Object for JSON Encoding.
        :return: Integer representation of Decimal else default JSON Conversion.
        """

        if isinstance(obj, Decimal):
            return int(obj)
        return super().default(obj)


class ViewpointStatusTable:
    """
    A class used to represent the ViewpointStatusTable.
    """

    def __init__(self, aws_ddb: ServiceResource, logger: Logger = getLogger(__name__)) -> None:
        """
        Initialize the ViewpointStatusTable and validate the table status.

        :param aws_ddb: An instance of the AWS DynamoDB service resource.
        :param logger: An optional logger to use.  If none provided it creates a new one.
        :return: None
        :raises ClientError: If the table does not exist in the specified AWS region.
        """
        self.ddb = aws_ddb
        self.table = self.ddb.Table(ServerConfig.viewpoint_status_table)
        self.logger = logger
        try:
            self.table.table_status
        except ClientError:
            self.logger.error(f"{ServerConfig.viewpoint_status_table} table does not exist in {ServerConfig.aws_region}.")
            raise

    def get_viewpoints(self, limit: int = None, next_token: str = None) -> ViewpointListResponse:
        """
        Get viewpoint items from the dynamodb table, if limit nor next_token are provided, it returns all records.

        :param limit: Optional max number of viewpoints requested from dynamodb.
        :param next_token: Optional token to begin a query from provided by the previous query response that
                had more records available.
        :return: The list of available viewpoints in the table.
        """
        query_params = {"FilterExpression": Attr("viewpoint_status").ne("DELETED")}
        if limit:
            query_params["Limit"] = limit
        if next_token:
            query_params["ExclusiveStartKey"] = {"viewpoint_id": next_token}
        try:
            if {"Limit", "ExclusiveStartKey"}.intersection(set(query_params.keys())):
                return self.get_paged_viewpoints(query_params)
            return self.get_all_viewpoints(query_params)
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

    def get_all_viewpoints(self, query_params: Dict) -> ViewpointListResponse:
        """
        Get all the viewpoint items from the dynamodb table.

        :param: query_params: A query to get all the viewpoints from the table.
        :return: A list of all the viewpoints found in the table.
        """
        response = self.table.scan(**query_params)
        data = response["Items"]
        while response.get("LastEvaluatedKey"):
            query_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = self.table.scan(**query_params)
            data.extend(response["Items"])
        return ViewpointListResponse(items=data)

    def get_paged_viewpoints(self, query_params: Dict) -> ViewpointListResponse:
        """
        Get a page of viewpoint items from the dynamodb table.

        :param: The query parameters to use for constructing the page of viewpoints.
        :return: Page of viewpoints associated with the query parameters.
        """
        response = self.table.scan(**query_params)
        ret_val = ViewpointListResponse(items=response["Items"])
        if response.get("LastEvaluatedKey"):
            ret_val = ViewpointListResponse(items=response["Items"], next_token=response["LastEvaluatedKey"]["viewpoint_id"])
        return ret_val

    def get_viewpoint(self, viewpoint_id: str) -> ViewpointModel:
        """
        Get detail of a viewpoint based on a given viewpoint id from the table.

        :param viewpoint_id: The viewpoint_id you want to get from the table.
        :return: Viewpoint details associated with the requested viewpoint_id.
        :raises: HTTPException if it cannot fetch a viewpoint item from the ViewpointStatusTable.
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
        Create a viewpoint item and store them in a DynamoDB table.

        :param viewpoint_request: The provided request to create a new viewpoint in the table.
        :return: Details of the newly created viewpoint item.
        :raises: HTTPException if it cannot create a viewpoint item from the ViewpointStatusTable.

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
        Update viewpoint item in a dynamodb table.

        :param viewpoint_item: Viewpoint item to be updated in the table.
        :return: Updated viewpoint item in the table.
        :raises: HTTPException if it cannot update a viewpoint item from the ViewpointStatusTable.

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

    def delete_viewpoint(self, viewpoint_id: str) -> str:
        """
        Delete a viewpoint from the DynamoDB table.

        :param viewpoint_id: The ID of the viewpoint to be deleted.
        :return: the viewpoint_id of the deleted viewpoint.
        :raises HTTPException: If an error occurs while deleting the viewpoint.
        """
        try:
            self.table.delete_item(Key={"viewpoint_id": viewpoint_id})
            return viewpoint_id
        except ClientError as err:
            raise HTTPException(
                status_code=err.response["Error"]["Code"],
                detail=f"Cannot delete viewpoint from ViewpointStatusTable, error: {err.response['Error']['Message']}",
            )
        except Exception as err:
            raise HTTPException(
                status_code=500,
                detail=f"Something went wrong when deleting a viewpoint from ViewpointStatusTable! Error: {err}",
            )

    @staticmethod
    def get_update_params(body: Dict) -> Tuple[str, Dict[str, Any]]:
        """
        Generate an update expression and a dict of values to update a dynamodb table.

        :param body: Body of the request that contains the updated data.
        :return: Generated update expression and attributes.
        """
        update_expr = ["SET"]
        update_attr = dict()

        for key, val in body.items():
            if key != "viewpoint_id":
                update_expr.append(f" {key} = :{key},")
                update_attr[f":{key}"] = val

        return "".join(update_expr)[:-1], update_attr
