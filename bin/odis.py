#!/usr/bin/env python3
import argparse
import os
import sys
from io import StringIO
from pprint import PrettyPrinter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from dotenv import load_dotenv

from common.config import load_config
from common.data_source_model import APIModel, DataSourceModel, DomainModel
from common.utils.factory.extractor_factory import create_extractor
from common.utils.factory.loader_factory import create_loader
from common.utils.file_handler import FileHandler
from common.utils.logging_odis import logger

load_dotenv()

# this module is the entry point for the CLI
# it will parse the arguments and execute the requested operation


DEFAULT_CONFIGFILE = "datasources.yaml"


def parse_args():
    """Defines the Argument Parser and returns parsed args"""

    # Define Argument Parser
    parser = argparse.ArgumentParser(description="Extract data from configured sources")
    parser.add_argument(
        "operation", action="store", help="Action to be taken : extract, load, explain"
    )
    parser.add_argument("-d", "--domain", help="Extract data for specific domain only")
    parser.add_argument(
        "-s", "--sources", help="List of sources to extract from", nargs="*"
    )
    parser.add_argument("-f", "--file", help="Path to an input file")
    parser.add_argument(
        "--config", default=DEFAULT_CONFIGFILE, help="Path to datasources config file"
    )
    parser.add_argument("--apis", help="APIs to be explained", nargs="*")

    return parser.parse_args()


def explain(
    config: DataSourceModel,
    apis: list[APIModel] = None,
    models: list[DomainModel] = None,
):

    output = StringIO()
    pp = PrettyPrinter(indent=4)

    # Pretty print information about the APIs
    output.write("\n\n================= API explanations =================\n")
    output.write(f"Available APIs from config : {list(config.APIs.keys())}\n")

    if apis is not None:
        for a in apis:
            output.write(f"\n~~~~~~ {a.name} ~~~~~~")
            output.write(f"{a.model_dump_json(indent=4)}\n")

            used_in = config.get_domains_with_models_for_api(a.name)
            output.write(
                f"This API is used in the following models : {pp.pformat(used_in)}\n"
            )

    # Pretty print information about the domains and source models
    output.write(
        "\n\n================= Domain & Models explanations =================\n"
    )
    output.write(f"Available Domains from config : {list(config.domains.keys())}\n")
    output.write(f"Available Models from config : {list(config.get_models().keys())}\n")

    if models is not None:
        for m in models:
            output.write(f"\nSOURCE MODEL: {m.name}\n")
            output.write(f"{m.model_dump_json(indent=4)}\n")

    print(output.getvalue())


def extract(
    config: DataSourceModel,
    models: list[DomainModel],
    **kwargs,
):

    for model in models:

        try:

            extractor = create_extractor(config, model, handler=FileHandler())
            extractor.execute()

        except Exception as e:

            logger.exception(f"Error extracting data from {model.name}: {str(e)}")


def load(
    config: DataSourceModel,
    models: list[DomainModel],
    **kwargs,
):

    for model in models:

        try:

            loader = create_loader(config, model, handler=FileHandler())
            loader.execute()

        except Exception as e:

            logger.exception(f"Issue in loading data : {e}")


if __name__ == "__main__":

    try:

        args = parse_args()

        # load config file
        config_name = args.config
        valid_config = load_config(config_name, response_model=DataSourceModel)

        # parse the api selected by the user
        # depending on the user input, we may need to filter the apis
        # if no api is given, we will use all apis
        # the user input is case insensitive
        apis = (
            [
                v
                for k, v in valid_config.APIs.items()
                if k.casefold()
                in [a.casefold() for a in args.apis]  # case insensitive comparison
            ]
            if args.apis
            else []
        )

        # parse the models to be used
        # depending on the user input, we may need to filter the models
        models: list[DomainModel] = []

        # if domain is given, we only want the models from that domain
        # if sources are given, we only want the models from those sources
        if args.domain:
            models = list(valid_config.get_models(args.domain).values())
        elif args.sources:
            models = list(
                {
                    k: v
                    for k, v in valid_config.get_models().items()
                    if k in args.sources
                }.values()
            )
        else:
            models = list(valid_config.get_models().values())  # get all models

        # load the function to be executed
        operation_func = locals()[args.operation]

    except Exception as e:
        logger.exception(f"Error preparing CLI arguments: {e}")
        sys.exit(1)

    # execute operation with args
    operation_func(
        config=valid_config,
        apis=apis,
        models=models,
    )
