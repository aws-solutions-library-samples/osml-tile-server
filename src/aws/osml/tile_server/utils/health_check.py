#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from pydantic import BaseModel


class HealthCheck(BaseModel):
    """
    A Pydantic model for a health check response.

    Attributes:
        status (str): Status of the health check. Defaults is "OK".
    """

    status: str = "OK"
