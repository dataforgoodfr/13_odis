from dataclasses import dataclass, field
from typing import Annotated, Literal, Optional, Self
import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    model_validator,
)

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
    throttle: Optional[int] = 60


class DomainModel(BaseModel):
    """the domain section of the yaml file"""

    API: str
    type: str
    endpoint: EndPoint
    description: Optional[str] = None

    params: Optional[dict] = Field(
        default=None,
        examples=[{"key": "value", "key2": 1.2}],
        description="arbitrary query parameters passed to the API",
    )

    response_map: Optional[dict] = Field(
        default={},
        examples=[{"next": "paging.next"}],
        description="mapping of response keys to domain-specific keys",
    )

    format: Literal["csv", "xlsx", "json"] = "json"


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
    

@dataclass
class PageLog():
    
    pageno: int
    filepath: Optional[str] # to be enhanced ; needs to be a valid path

@dataclass
class DataProcessLog():
    """model for exchanging processing reports between extractors and loaders"""

    domain: str
    source: str
    operation: str
    source_config: dict = None
    last_run_time: str = field(default_factory = lambda: datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
    last_page: int = 0
    successfully_completed: bool = False
    pages: list[PageLog] = field(default_factory = list)

    @classmethod
    def from_dict(dict_data):
        
        pagelogs = []
        for page in dict_data.get('pages'):
            page_obj = PageLog(
                pageno = page['pageno'],
                filepath = page['filepath']
            )
            pagelogs.append(page_obj)

        return DataProcessLog(
            domain = dict_data.get('domain'),
            source = dict_data.get('source'),
            operation = dict_data.get('operation'),
            last_run_time = dict_data.get('last_run_time'),
            last_page = dict_data.get('last_page'),
            successfully_completed = dict_data.get('successfully_completed'),
            pages = pagelogs
        )
    
    def to_dict(self):
        
        raw_dict = self.__dict__
        serialized_pagelogs = []
        for pagelog in self.pages:
            serialized_pagelogs.append(pagelog.__dict__)

        # replace the PageLog list by serializable list(dict)
        raw_dict.pop('pages')
        serialized_dict = raw_dict
        serialized_dict['pages'] = serialized_pagelogs

        return serialized_dict

    def add_pagelog(self, pageno: int, filepath:str = None, is_last:bool = False):
        
        pagelog = PageLog(pageno = pageno, filepath = filepath)
        self.pages.append(pagelog)
        self.last_page = pageno
        self.successfully_completed = is_last

    