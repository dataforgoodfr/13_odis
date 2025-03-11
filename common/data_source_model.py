from typing import Annotated, Literal, Optional, Self

from pydantic import BaseModel, ConfigDict, HttpUrl, StringConstraints, model_validator

EndPoint = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        pattern=r"^\/.*",  # must start with a '/'
        min_length=1,
    ),
]


class HeaderModel(BaseModel):

    accept: Literal["application/json", "application/xml", "text/csv"] = (
        "application/json"
    )


class APIModel(BaseModel):
    """the API section of the yaml file"""

    name: str
    base_url: HttpUrl
    apidoc: Optional[HttpUrl] = None
    description: Optional[str] = None
    default_headers: Optional[HeaderModel] = None


class DomainParamsModel(BaseModel):
    """domain parameters"""

    withColumnName: bool = False
    withColumnDescription: bool = False
    withColumnUnit: bool = False
    page: Optional[int] = 1
    pageSize: Optional[Literal["all"]] = "all"


class DomainModel(BaseModel):
    """the domain section of the yaml file"""

    API: str
    type: str
    endpoint: EndPoint
    description: Optional[str] = None
    params: Optional[DomainParamsModel] = None
    format: Optional[Literal["csv", "json"]] = "json"


class ConfigurationModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class DataSourceModel(ConfigurationModel):
    """global model for the yaml file"""

    APIs: dict[str, APIModel]
    domains: dict[str, dict[str, DomainModel]]

    # when loading the domains, we need to check that the API is in the APIs dict
    # this is done by the validator
    @model_validator(mode="after")
    def check_domain_api(self) -> Self:
        """
        verify the domain API is in the APIs dict
        """
        for domain in self.domains.values():
            for subdomain in domain.values():
                if subdomain.API not in self.APIs:
                    raise ValueError(f"API '{subdomain.API}' not found in APIs section")

        return self
        return self
        return self
