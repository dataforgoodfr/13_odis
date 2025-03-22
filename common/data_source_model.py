import datetime
from dataclasses import dataclass, field
from typing import Annotated, Literal, Optional, Self

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

FILE_FORMAT = Literal["csv", "json", "xlsx"]


class HeaderModel(BaseModel):

    model_config = ConfigDict(extra="allow")  # allow extra headers

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
    """the domain section of the yaml file

    when no headers are provided, default headers are used
    and are merged with the API default headers

    Example:
    ```python
    DomainModel(
        API="INSEE.Metadonnees",
        type="json",
        endpoint="/regions",
        description="Regions in France",
        headers=HeaderModel(accept="application/json"),
        params={"key": "value"},
        response_map={"next": "paging.next"},
        format="json",
    )
    ```

    """

    API: str
    type: str
    endpoint: EndPoint
    description: Optional[str] = None
    headers: Optional[HeaderModel] = Field(
        default_factory=HeaderModel,
        description="headers to be sent with the request",
    )

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

    format: Optional[FILE_FORMAT] = "json"

    name: Optional[str] = Field(
        default=None,
        description="""
            a human-readable name for the model
        """,
    )

    def merge_headers(self, api_headers: HeaderModel) -> Self:
        """
        local headers are merged with the API headers

        the local headers have precedence over the API headers
        """
        if self.headers:
            if api_headers:
                d = api_headers.model_dump(mode="json")
                d.update(self.headers.model_dump(mode="json"))
                self.headers = HeaderModel(**d)
        else:
            self.headers = api_headers

        return self


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
=======
>>>>>>> d813691 (merged with main branch)

    @model_validator(mode="after")
    def merge_model_headers(self) -> Self:
        """
        API default headers are merged with the domain headers
        """
        for domain in self.domains.values():
            for model in domain.values():
                api = self.APIs[model.API]
                model.merge_headers(api.default_headers)

        return self

    @model_validator(mode="after")
    def set_model_name(self) -> Self:
        """
        set the name of a model in a domain,
        the name is the concatenation of the domain and the model name

        Example:
        ```python
        {
            "Metadonnees": {
                "INSEE": DomainModel(...),
            },
        }
        # name would be Metadonnees.INSEE
        ```

        """
        for d_name, domain in self.domains.items():
            for m_name, model in domain.items():
                model.name = f"{d_name}.{m_name}"
        return self

    def get_domains_with_models_for_api(self, api_name: str) -> dict[str, list[str]]:
        """
        get the domains and models that use the given API name,

        the result is a dict with the domain name as key and the models list using the API as value

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

    def list_domains_names(self) -> list[str]:
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
            f"{model.name}": model
            for d in self.domains.values()
            for model in d.values()
        }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9fb78f0 (test & refacto explain_source)
<<<<<<< HEAD
>>>>>>> 14d9d61 (test & refacto explain_source)
=======
=======
=======
>>>>>>> d813691 (merged with main branch)

    def get_domain_name(self, model: DomainModel) -> str:
        """provides the domain name for a given model

        Example:
        ```python

        # config is a DataSourceModel instance
        # assuming the domains are "Metadonnees" and "Geographical" with models "INSEE" and "IGN"

        config.get_domain_by_model(DomainModel(...))
        # output
        # "Metadonnees"
        # means that the model is in the "Metadonnees" domain
        ```
        """
        for domain_name, domain in self.domains.items():
            if model in domain.values():
                return domain_name

        raise ValueError(f"Model '{model}' not found in domains")

    def get_api(self, model: DomainModel) -> APIModel:
        """provides the `APIModel` for a given `DomainModel`

        Example:
        ```python

        # config is a DataSourceModel instance
        # assuming the domains are "Metadonnees" and "Geographical" with models "INSEE" and "IGN"

        config.get_api(DomainModel(...))
        # output
        # APIModel(...)
        ```
        """

        for key, api in self.APIs.items():
            if model.API == key:
                return api

        raise ValueError(f"API '{model.API}' not found in APIs section")
<<<<<<< HEAD
>>>>>>> 42ef62f (refacto: tmp waiting for tests)
>>>>>>> d8c4deb (refacto: tmp waiting for tests)
=======


@dataclass
class PageLog:

    pageno: int
    filepath: Optional[str]  # to be enhanced ; needs to be a valid path


@dataclass
class DataProcessLog:
    """model for exchanging processing reports between extractors and loaders"""

    domain: str
    source: str
    operation: str
    last_run_time: str = field(
        default_factory=lambda: datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    )
    last_page: int = 0
    successfully_completed: bool = False
    pages: list[PageLog] = field(default_factory=list)

    @classmethod
    def from_dict(dict_data):

        pagelogs = []
        for page in dict_data.get("pages"):
            page_obj = PageLog(pageno=page["pageno"], filepath=page["filepath"])
            pagelogs.append(page_obj)

        return DataProcessLog(
            domain=dict_data.get("domain"),
            source=dict_data.get("source"),
            operation=dict_data.get("operation"),
            last_run_time=dict_data.get("last_run_time"),
            last_page=dict_data.get("last_page"),
            successfully_completed=dict_data.get("successfully_completed"),
            pages=pagelogs,
        )

    def to_dict(self):

        raw_dict = self.__dict__
        serialized_pagelogs = []
        for pagelog in self.pages:
            serialized_pagelogs.append(pagelog.__dict__)

        # replace the PageLog list by serializable list(dict)
        raw_dict.pop("pages")
        serialized_dict = raw_dict
        serialized_dict["pages"] = serialized_pagelogs

        return serialized_dict

    def add_pagelog(self, pageno: int, filepath: str = None, is_last: bool = False):

        pagelog = PageLog(pageno=pageno, filepath=filepath)
        self.pages.append(pagelog)
        self.last_page = pageno
        self.successfully_completed = is_last
>>>>>>> d813691 (merged with main branch)
