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
    """model for easily updating and logging information about the processing of a given page"""
    
    pageno: int
    filepath: Optional[str] = None # to be enhanced ; needs to be a valid path
    is_last: Optional[bool] = False
    extracted: Optional[bool] = False
    bronze_loaded: Optional[bool] = False

@dataclass
class DataProcessLog():
    """model for exchanging processing reports between extractors and loaders"""

    domain: str
    source: str
    operation: str
    last_run_time: str = field(default_factory = lambda: datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
    exctracted_pages: int = 0
    loaded_pages: int = 0
    successfully_completed: bool = False
    source_config: dict = None
    pages: dict[PageLog] = field(default_factory = dict)

    @classmethod
    def from_dict(cls, dict_data):
        
        processed_dict_data = dict_data

        # Pop out dict-type pagelogs to reinsert PageLog types
        pagelogs = {}
        pages_dict = processed_dict_data.pop('pages')
        for pageno, pagelog_dict in pages_dict.items():
            pagelog = PageLog( **pagelog_dict )
            pagelogs[pageno] = pagelog
        
        processed_dict_data['pages'] = pagelogs

        return cls( **processed_dict_data )
    
    def to_dict(self):
        
        raw_dict = self.__dict__
        serialized_pagelogs = {}
        for pageno, pagelog in self.pages.items():
            serialized_pagelogs[pageno] = pagelog.__dict__

        # replace the PageLog list by serializable list(dict)
        raw_dict.pop('pages')
        serialized_dict = raw_dict
        serialized_dict['pages'] = serialized_pagelogs

        return serialized_dict

    def update_pagelog(self, pageno: int, **pagelog_params):
        """Creates or Updates a PageLog with the information of how its last processing (extract or load) went"""
        
        pagelog = self.pages.get(str(pageno))

        if pagelog is None:
            # if pagenumber was not found, create a new PageLog
            pagelog = PageLog(pageno, **pagelog_params )
            
        else:
            # Update PageLog in place
            for key, value in pagelog_params.items():
                setattr(pagelog, key, value)
        
        # Add page log to pages
        self.pages[pageno] = pagelog

        # If relevant, increment the extracted pages count
        if self.operation == 'extract' and pagelog.extracted:
            self.exctracted_pages += 1

        # If relevant, increment the loaded pages count
        if self.operation == 'load' and pagelog.bronze_loaded:
            self.loaded_pages += 1

        # If this page was the last, then infer process is successful and complete
        self.successfully_completed = pagelog.is_last

        
    