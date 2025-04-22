#!/usr/bin/env python3

import os
import sys
from typing import Annotated

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import typer
from rich import print
from rich.console import Console
from rich.table import Table

from common.config import load_config
from common.data_source_model import APIModel, DataSourceModel, DomainModel
from common.utils.factory.extractor_factory import create_extractor
from common.utils.factory.loader_factory import create_loader
from common.utils.file_handler import FileHandler
from common.utils.logging_odis import logger

# this module is the entry point for the CLI
# it will parse the arguments and execute the requested operation


DEFAULT_CONFIGFILE = "datasources.yaml"

# options for the CLI
OPTION_ALL = "*"
OPTION_NONE = ""

app = typer.Typer()
console = Console()


def explain_data_source(config_model: DataSourceModel, source: str):

    table = Table("Domain", "Data Source Name", "Description")

    for k, d in config_model.domains.items():

        for m in d.values():
            if m.name != source:
                continue
            table.add_row(
                k,
                m.name,
                m.description,
            )

            print(m.model_dump(mode="json"))
            print("\n")

    console.print(table)


def explain_domain(config_model: DataSourceModel, domain: str):
    table = Table("Domain", "Data Source Name", "Description")

    for k, d in config_model.domains.items():
        if k != domain:
            continue

        for m in d.values():
            table.add_row(
                k,
                m.name,
                m.description,
            )

    console.print(table)


def explain_api(config_model: DataSourceModel, api: str):

    apis_list = apis_from_str(config_model, apis=api)

    table = Table("API", "Description", "Used by data source")
    for a in apis_list:

        used_in = config_model.get_domains_with_models_for_api(
            a.name
        )  # get the models using the API

        for d in used_in.keys():
            ds = config_model.get_models(d)  # get the models using the API
            table.add_row(
                a.name,
                a.description,
                "\n".join([f"- {s.name}" for s in ds.values()]),
            )
            table.add_section()

    console.print(table)


@app.command()
def explain(
    api: Annotated[
        str,
        typer.Option(
            "-a",
            "--api",
            help="API to be explained",
            metavar="api_1",
        ),
    ] = OPTION_NONE,
    domain: Annotated[
        str,
        typer.Option(
            "-d",
            "--domain",
            help="domain to be explained",
            metavar="domain_1",
        ),
    ] = OPTION_NONE,
    source: Annotated[
        str,
        typer.Option(
            "-s",
            "--source",
            help="data source to be explained",
            metavar="source_1",
        ),
    ] = OPTION_NONE,
    config: Annotated[
        str | None,
        typer.Option(
            "-c",
            "--config",
            help=f"Path to the config file, defaults to {DEFAULT_CONFIGFILE}",
            metavar="path",
        ),
    ] = DEFAULT_CONFIGFILE,
):
    """
    Explain the APIs and data sources in the config file.
    This function will print the APIs and data sources in the config file
    and their descriptions.

    """

    if not os.path.exists(config) or not os.path.isfile(config):
        print(f"[red]Config file {config} does not exist or is not a file.[/red]")
        sys.exit(1)

    config_model: DataSourceModel = load_config(config, response_model=DataSourceModel)

    ## ---------------------------------------
    ## case where the user wants to have an overview
    if api == OPTION_NONE and source == OPTION_NONE and domain == OPTION_NONE:

        print("\n")
        print("[green]Overview of the APIs and data sources[/green]")
        print("\n")

        table = Table("API", "Name", "Description")
        for k, m in config_model.APIs.items():
            table.add_row(
                k,
                m.name,
                m.description,
            )

        print("[green]List of APIs[/green]")
        console.print(table)

        table = Table("Data Source", "Description")
        for _, d in config_model.get_models().items():
            table.add_row(
                d.name,
                d.description,
            )

        print("[green]List of Data sources[/green]")
        console.print(table)

    ## ---------------------------------------
    ## case where the user wants to see a precise APIs
    ## without specifying the data source
    if api != OPTION_NONE and source == OPTION_NONE:

        print("\n")
        print(f"[green]Viewing API '{api}'[/green]")
        print("\n")

        explain_api(config_model, api)

    ## ---------------------------------------
    ## case where the user specifies the API and the data source
    if api != OPTION_NONE and source != OPTION_NONE:

        print("\n")
        print(f"[green]Viewing API '{api}'[/green]")
        print("\n")

        explain_api(config_model, api)

        print("\n")
        print(f"[green]Viewing data source '{source}'[/green]")
        print("\n")

        explain_data_source(config_model, source)

    ## ---------------------------------------
    ## case where the user wants to see a precise Domain
    if domain != OPTION_NONE:

        print("\n")
        print(f"[green]Viewing data sources of domain '{domain}'[/green]")
        print("\n")

        explain_domain(config_model, domain)

    ## ---------------------------------------
    ## case where the user wants to see a precise Data Source only
    if source != OPTION_NONE and api == OPTION_NONE:

        print("\n")
        print(f"[green]Viewing Data source '{source}'[/green]")
        print("\n")

        explain_data_source(config_model, source)
    ## ---------------------------------------


@app.command()
def extract(
    source: Annotated[
        str,
        typer.Option(
            "-s",
            "--source",
            help="comma separated list of data sources to be extracted, special value '*' for all data sources",
            metavar="source_1,source_2",
        ),
    ] = OPTION_NONE,
    domain: Annotated[
        str,
        typer.Option(
            "-d",
            "--domain",
            help="comma separated list of domains to be extracted, special value '*' for all domains",
            metavar="domain_1,domain_2",
        ),
    ] = OPTION_NONE,
    config: Annotated[
        str | None,
        typer.Option(
            "-c",
            "--config",
            help=f"Path to the config file, defaults to {DEFAULT_CONFIGFILE}",
            metavar="path",
        ),
    ] = DEFAULT_CONFIGFILE,
):
    """
    Extract data from the data sources specified in the config file.
    This function will extract data from the data sources specified in the config file
    \n
    - if the user specifies the sources, it will extract data from the specified sources\n
    - if the user specifies the domains, it will extract data from the data sources in the specified domains\n
    - if the user specifies both, it will extract data from the specified sources and domains
    \n
    At least one of the two options must be specified.
    """

    if source == OPTION_NONE and domain == OPTION_NONE:
        print(
            "[red]You must specify at least one of the two options: --source or --domain[/red]"
        )
        sys.exit(1)

    config_model: DataSourceModel = load_config(config, response_model=DataSourceModel)

    data_sources = []

    # get the data sources from the domains
    if domain is not None:
        data_sources = data_sources_from_domains_str(config_model, domains=domain)

    # eventually, get the data sources from the sources
    # if the user specified the sources, we want to add them to the list
    if source is not None:
        data_sources.extend(data_sources_from_str(config_model, sources=source))

    if len(data_sources) == 0:
        print(
            "[red]No data sources found, please check the config file and the options provided.[/red]"
        )
        sys.exit(1)

    print(
        f"[green]Extracting data from the following data sources: {[ds.name for ds in data_sources]}[/green]"
    )

    with typer.progressbar(data_sources) as progress:

        is_exception = False

        for ds in progress:

            try:

                print("\n")
                print("\n[blue]Using data source configuration:[/blue]")
                explain_data_source(config_model, ds.name)
                print("\n")

                print(f"\n[blue]Extracting data from {ds.name}[/blue]")
                print("\n")

                extractor = create_extractor(config_model, ds, handler=FileHandler())
                extractor.execute()

                print(f"\n[blue]Data extracted from {ds.name}[/blue]")
                print("\n")

            except Exception as e:

                is_exception = True
                logger.exception(f"Error extracting data from {ds.name}: {str(e)}")

    if is_exception:
        print(
            "[red]There was an issue in extracting data, please check the logs for more details.[/red]"
        )
        # exit with a non-zero status code
        # to indicate that there was an error
        sys.exit(1)
    else:
        print("\n")
        print("[green]All data extracted successfully[/green]")
        print("\n")


@app.command()
def load(
    source: Annotated[
        str,
        typer.Option(
            "-s",
            "--source",
            help="comma separated list of data sources to be loaded, special value '*' for all data sources",
            metavar="source_1,source_2",
        ),
    ] = OPTION_NONE,
    domain: Annotated[
        str,
        typer.Option(
            "-d",
            "--domain",
            help="comma separated list of domains to be loaded, special value '*' for all domains",
            metavar="domain_1,domain_2",
        ),
    ] = OPTION_NONE,
    config: Annotated[
        str | None,
        typer.Option(
            "-c",
            "--config",
            help=f"Path to the config file, defaults to {DEFAULT_CONFIGFILE}",
            metavar="path",
        ),
    ] = DEFAULT_CONFIGFILE,
):

    if source == OPTION_NONE and domain == OPTION_NONE:
        print(
            "[red]You must specify at least one of the two options: --source or --domain[/red]"
        )
        sys.exit(1)

    data_sources = []

    config_model: DataSourceModel = load_config(config, response_model=DataSourceModel)

    # get the data sources from the domains
    if domain is not None:
        data_sources = data_sources_from_domains_str(config_model, domains=domain)

    # eventually, get the data sources from the sources
    # if the user specified the sources, we want to add them to the list
    if source is not None:
        data_sources.extend(data_sources_from_str(config_model, sources=source))

    if len(data_sources) == 0:
        print(
            "[red]No data sources found, please check the config file and the options provided.[/red]"
        )
        sys.exit(1)

    print(
        f"[green]Loading data from the following data sources: {[ds.name for ds in data_sources]}[/green]"
    )

    with typer.progressbar(data_sources) as progress:

        is_exception = False

        for ds in progress:

            try:

                print("\n")
                print("\n[blue]Using data source configuration:[/blue]")
                explain_data_source(config_model, ds.name)
                print("\n")

                print(f"\n[blue]Loading data into {ds.name}[/blue]")

                loader = create_loader(config_model, ds, handler=FileHandler())
                loader.execute()

                print(f"[blue]Data loaded into {ds.name}[/blue]")

            except Exception as e:

                logger.exception(f"Issue in loading data : {e}")
                is_exception = True

    if is_exception:
        print(
            "[red]There was an issue in loading data, please check the logs for more details.[/red]"
        )
        # exit with a non-zero status code
        # to indicate that there was an error
        sys.exit(1)
    else:
        print("\n")
        print("[green]All data loaded successfully[/green]")
        print("\n")


def apis_from_str(
    config: DataSourceModel,
    apis: str | None = OPTION_NONE,
) -> list[APIModel]:
    """
    Parse the `apis` string and returns a list of corresponding APIModel objects.

    Args:
        config (DataSourceModel): The config object containing the APIs
        apis (str | None): a comma separated list of APIs

    Returns:
        list[APIModel]: A list of APIModel objects
    """

    apis_list = []

    if apis is not None and apis != OPTION_NONE:
        # Split the comma separated string into a list
        apis_list = [api.strip() for api in apis.split(",")]

    elif apis == OPTION_NONE:
        # If the user input is "*", we want all APIs
        apis_list = list(config.APIs.keys())

    apis = (
        [
            v
            for k, v in config.APIs.items()
            if k.casefold()
            in [a.casefold() for a in apis_list]  # case insensitive comparison
        ]
        if apis_list
        else []
    )

    return apis


def data_sources_from_str(
    config: DataSourceModel,
    sources: str | None = OPTION_NONE,
) -> list[DomainModel]:
    """
    Parse the `sources` string and returns a list of corresponding DomainModel objects.

    Args:
        config (DataSourceModel): The config object containing the sources
        sources (str | None): A comma separated list of sources

    Returns:
        list[DomainModel]: A list of DomainModel objects

    """

    sources_list = []
    if sources is not None and sources != OPTION_ALL:
        # Split the comma separated string into a list
        sources_list = [source.strip() for source in sources.split(",")]

    elif sources == OPTION_ALL:
        # If the user input is "*", we want all sources
        sources_list = list(config.get_models().keys())

    data_sources = list(
        {k: v for k, v in config.get_models().items() if k in sources_list}.values()
    )

    return data_sources


def data_sources_from_domains_str(
    config: DataSourceModel,
    domains: str | None = OPTION_NONE,
) -> list[DomainModel]:
    """
    Parse the `domains` string and returns a list of corresponding DomainModel objects.

    Args:
        config (DataSourceModel): The config object containing the sources
        domains (str | None): A comma separated list of domains

    Returns:
        list[DomainModel]: A list of DomainModel objects

    """

    if domains is not None and domains != OPTION_ALL:

        # Split the comma separated string into a list
        domains_list = [domain.strip() for domain in domains.split(",")]

    elif domains == OPTION_ALL:
        # If the user input is "*", we want all sources
        domains_list = list(config.domains.keys())

    data_sources = []
    for d in domains_list:
        try:
            data_sources.extend(list(config.domains[d].values()))
        except KeyError:
            print("\n")
            print(
                f"[bold][red]Domain '{d}' not found in the config file, skipping it[/red][/bold]"
            )
            print("\n")

    return data_sources


if __name__ == "__main__":
    app()
