#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Optional

from pydantic import BaseModel, Field


class ConformanceDeclaration(BaseModel):
    """
    This class defines the resource returned from the /conformance path in an OGC compliant API.
    """

    conforms_to: list[str] = Field(
        serialization_alias="conformsTo",
        description="An array of URIs. Each  URI should correspond to a defined OGC conformance class. "
        "Unrecognized URIs should be ignored.",
    )


class Link(BaseModel):
    """
    OGC Web API Standards use RFC 8288 (Web Linking) to express relationships between resources. Resource
    representations defined in these Standards commonly include a “links” element. A “links” element is an array of
    individual hyperlink elements. These “links” elements provide a convention for associating related resources.

    The individual hyperlink elements that make up a “links” element are defined using this class.
    """

    href: str = Field(description="Supplies the URI to a remote resource (or resource fragment).")
    rel: Optional[str] = Field(description="The type or semantics of the relation.", default=None)
    type: Optional[str] = Field(
        description="A hint indicating what the media type of the result of dereferencing the link should be.", default=None
    )
    templated: Optional[bool] = Field(description="This flag set to true if the link is a URL template.", default=None)
    var_base: Optional[str] = Field(
        alias="varBase",
        description="A base path to retrieve semantic information about the variables used in URL template.",
        default=None,
    )
    hreflang: Optional[str] = Field(
        description="A hint indicating what the language of the result of dereferencing the link should be.", default=None
    )
    title: Optional[str] = Field(
        description="Used to label the destination of a link such that it can be used as a human-readable identifier.",
        default=None,
    )
    length: Optional[int] = Field(default=None)


class LandingPage(BaseModel):
    """
    This class defines a common landing page for an OGC API.
    """

    title: Optional[str] = Field(
        description="The title of the API. "
        "While the title is not required, implementers are strongly advised to include one.",
        default=None,
    )
    description: Optional[str] = Field(description="A textual description of the API.", default=None)
    attribution: Optional[str] = Field(
        description="The attribution for the API. "
        "The attribution should be short and intended for presentation to a user on a UI. "
        "Parts of the text can be links to other resources if additional information is needed. "
        "The string can include HTML markup.",
        default=None,
    )
    links: list[Link] = Field(description="Links to the resources exposed through this API.")


class ExceptionResponse(BaseModel):
    """
    OGC schema for exceptions based on RFC 7807.
    """

    type: str = Field(description="A URI reference that identifies the problem type.")
    title: Optional[str] = Field(description="A short, human-readable summary of the problem type.", default=None)
    status: Optional[int] = Field(
        description="The HTTP status code generated by the origin server for this occurrence of the problem.", default=None
    )
    detail: Optional[str] = Field(
        description="A human-readable explanation specific to this occurrence of the problem.", default=None
    )
    instance: Optional[str] = Field(
        description="A URI reference that identifies the specific occurrence of the problem."
        " It may or may not yield further information if dereferenced.",
        default=None,
    )
