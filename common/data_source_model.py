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
            for model in domain.values():
                if model.API not in self.APIs:
                    raise ValueError(f"API '{model.API}' not found in APIs section")

        return self
<<<<<<< HEAD
    

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

<<<<<<< HEAD
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

        
    
=======
    
=======

    def get_api_domains(self, api_name: str) -> dict[str, list[str]]:
        """
        get the domains that use the given API name,
        the result is a dict with the domain name as key and the subdomain names list using the API as value

        Example:
        ```python

        # config is a DataSourceModel instance
        config.get_api_domains("INSEE.Metadonnees")
        # output
        # {
        #     "geographical_references": ["regions", "departments"],
        # }
        # means that the API "INSEE.Metadonnees" is used in the "geographical_references" domain
        # for the "regions" and "departments" models
        ```

        """
        return {
            domain_name: [
                model_name
                for model_name, model in domain.items()
                if model.API == api_name
            ]
            for domain_name, domain in self.domains.items()
        }

    def list_domains(self) -> list[str]:
        return list(self.domains.keys())

    def get_models(self, domain: str = None) -> dict[str, DomainModel]:
        """provides the list of models for a given domain, or all models if no domain is provided

        when no domain is provided, the key is the concatenation of the domain and the model name

        Example:
        ```python

        # config is a DataSourceModel instance
        # assuming the domains are "Metadonnees" and "Geographical" with models "INSEE" and "IGN"

        config.get_models()
        # output
        # {
        #     "Metadonnees.INSEE": DomainModel(...),
        #     "Geographical.INSEE": DomainModel(...),
        #     "Geographical.IGN": DomainModel(...),
        # }
        # means that the "INSEE" domain has 2 models and the "IGN" domain has 1 model
        ```

        """
        if domain:
            return self.domains[domain]

        return {
            f"{name}.{model_name}": model
            for name, d in self.domains.items()
            for model_name, model in d.items()
        }
>>>>>>> 9fb78f0 (test & refacto explain_source)
>>>>>>> 14d9d61 (test & refacto explain_source)
