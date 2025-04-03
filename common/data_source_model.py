from typing import Annotated, Literal, Optional, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    StringConstraints,
    computed_field,
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


AcceptHeader = Annotated[
    str,
    StringConstraints(
        pattern=r"^((application\/json|application\/xml|text\/csv|application\/zip|application\/octet-stream|\*\/\*)(,\s?)?){1,}$"
    ),
]

FILE_FORMAT = Literal["csv", "json", "xlsx", "zip"]

Description = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),  # must not be empty string
]


class HeaderModel(BaseModel):
    model_config = ConfigDict(extra="allow")  # allow extra headers
    accept: AcceptHeader = "application/json"


class DataLoadParameters(BaseModel):

    model_config = ConfigDict(extra="allow")  # allow extra headers

    separator: Optional[str] = Field(
        default=";",
        description="""
            Optional separator for CSV files.
            if not provided, the CSV loader will sniff the separator
        """,
    )

    header: Optional[int] = Field(
        default=0,
        description="""
            Used to specify the row number(s) to use as the column names,
            and the start of the data.
            see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        """,
    )

    skipfooter: Optional[int] = Field(
        default=0,
        description="""
            Optional number of lines to skip at the end of the file.
            if not provided, the CSV loader will not skip any lines
        """,
    )


class APIModel(BaseModel):
    """the API section of the yaml file"""

    name: str
    base_url: HttpUrl
    apidoc: Optional[HttpUrl] = None
    description: Optional[Description] = None
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
        extract_params={"key": "value"},
        load_params={"key": "value"},
        response_map={"next": "paging.next"},
        format="json",
    )
    ```

    """

    type: str = Field(
        ...,
        description="""
            type of the extractor to be used,
        """,
        examples=["NotebookExtractor", "JsonExtractor", "CSVExtractor"],
    )
    description: Description = Field(
        ...,
        description="""
            a human-readable description of the data source
        """,
    )
    format: Optional[FILE_FORMAT] = Field(
        default="json",
        description="""
            format of the data to be extracted,
            if not provided, the default format is `json`
        """,
    )
    name: Optional[str] = Field(
        default=None,
        description="""
            a human-readable name for the model
        """,
    )

    #################################
    # API related fields
    #################################
    API: Optional[str] = Field(
        default=None,
        description="""
            API name as defined in the APIs section,
            the API name is optional when the API is not used, 
            which is the case when using the type `NotebookExtractor`
        """,
    )
    endpoint: Optional[EndPoint] = Field(
        default=None,
        description="""
            endpoint of the API to be called,
            must start with a '/' and not end with one,
            The endpoint is optional when the API is not used,
            which is the case when using the type `NotebookExtractor`
        """,
        examples=["/regions", "/departements"],
    )

    headers: Optional[HeaderModel] = Field(
        default_factory=HeaderModel,
        description="headers to be sent with the request",
    )

    extract_params: Optional[dict] = Field(
        default=None,
        examples=[{"key": "value", "key2": 1.2}],
        description="arbitrary query parameters passed to the API",
    )

    load_params: Optional[DataLoadParameters] = Field(
        default_factory=DataLoadParameters,
        examples=[{"separator": ",", "header": 3}],
        description="""
            Parameters to be passed to the Loader when parsing the data,
            such as the separator for CSV files,
            the parameters are passed to the loader as keyword arguments,
        """,
    )

    response_map: Optional[dict] = Field(
        default={},
        examples=[{"next": "paging.next"}],
        description="mapping of response keys to domain-specific keys",
    )

    #################################
    # Notebook related fields
    #################################
    notebook_path: Optional[FilePath] = Field(
        default=None,
        description="""
            path to the notebook to be used for the extraction,
            the path is relative to the root of the repository,
            it is used when the type is `NotebookExtractor`
            the path must be a valid path and not a URL
            the path is ignored (unused) when the API is used,
        """,
        examples=["notebooks/regions.ipynb", "notebooks/departements.ipynb"],
    )

    def merge_headers(self, api_headers: HeaderModel) -> Self:
        """
        local headers are merged with the API headers

        the local headers have precedence over the API headers
        """
        if self.headers:
            if api_headers:
                d = api_headers.model_dump(mode="json")
                model_headers_dump = self.headers.model_dump(mode="json")
                d.update(model_headers_dump)
                self.headers = HeaderModel(**d)
        else:
            self.headers = api_headers

        return self

    @computed_field
    @property
    def table_name(self) -> str:
        """
        generate the DB table name storing data for this model,
        It is generated replacing the '.' in the model name by '_'

        Example:
        ```python
        DomainModel(name="logement.dido")
        # table_name would be "logement_dido"
        ```
        """
        if self.name:
            return self.name.replace(".", "_")
        return None

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        """
        verify the consistency of the model:
        - check that the API names are valid
        - check that model `notebook_path` is provided when the type is `NotebookExtractor`
        """

        if self.type == "NotebookExtractor":
            if self.notebook_path is None:
                raise ValueError(
                    "notebook_path must be provided when the type is 'NotebookExtractor'"
                )
            elif not self.notebook_path.is_file():
                raise ValueError(
                    "notebook_path must be a valid path to a notebook file"
                )
        else:
            if self.API is None:
                raise ValueError(
                    "API must be provided when the type is not 'NotebookExtractor'"
                )
            elif self.endpoint is None:
                raise ValueError(
                    "endpoint must be provided when the type is not 'NotebookExtractor'"
                )

        return self

    @computed_field
    @property
    def domain_name(self) -> str:
        """
        gets the domain name for the model

        Example:
        ```python
        DomainModel(name="logement.dido")
        # table_name would be "logement"
        ```
        """
        if self.name:
            return self.name.split(".")[0]
        return None


class ConfigurationModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class DataSourceModel(ConfigurationModel):
    """global model for the yaml file"""

    APIs: dict[str, APIModel] = Field(
        default={},
        description="""
            This section is optional since the domains can be used without an API,
        """,
        examples=[
            {
                "base_url": "https://api1.com",
                "name": "api1 name",
                "apidoc": "https://api1.com/doc",
                "description": "description of the API",
            },
        ],
    )
    domains: dict[str, dict[str, DomainModel]]

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        """
        verify the consistency of the datasource configuration:
        - check that the API names are valid if the model is not a NotebookExtractor
        """
        for domain in self.domains.values():
            for model in domain.values():
                if model.type != "NotebookExtractor" and model.API not in self.APIs:
                    raise ValueError(f"API '{model.API}' not found in APIs section")

        return self

    @model_validator(mode="after")
    def merge_model_headers(self) -> Self:
        """
        API default headers are merged with the domain headers
        for each model in the domain section, except for the NotebookExtractor
        the local headers have precedence over the API headers
        """
        for domain in self.domains.values():
            for model in domain.values():
                if model.type == "NotebookExtractor":
                    continue
                api = self.APIs.get(model.API, None)
                model.merge_headers(api.default_headers) if api is not None else None

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

    def get_model(self, model_name: str = None) -> DomainModel:
        """provides the model for a given model name
        Example:
        ```python
        # config is a DataSourceModel instance
        # assuming the domains are "Metadonnees" and "Geographical" with models "INSEE" and "IGN"
        config.get_model("Metadonnees.INSEE")
        # output
        # DomainModel(...)
        # means that the model "INSEE" is in the "Metadonnees" domain
        ```

        """
        if model_name:
            for domain in self.domains.values():
                for model in domain.values():
                    if model.name == model_name:
                        return model

        raise ValueError(f"Model '{model_name}' not found in domains")

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

        if the model is not using an API, None is returned

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

        return None
