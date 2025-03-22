#!/usr/bin/env python3
import argparse
import os
import sys
from io import StringIO
from pprint import PrettyPrinter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import load_config
from common.data_source_model import APIModel, DataSourceModel, DomainModel
from common.utils.extractor_factory import create_extractor
from common.utils.logging_odis import logger


def explain_source(
    config: DataSourceModel,
    apis: list[APIModel] = None,
    models: list[DomainModel] = None,
    default_all: bool = False,
) -> str:

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

    return output.getvalue()


def parse_args():

    parser = argparse.ArgumentParser(description="Extract data from configured sources")
    parser.add_argument("-d", "--domain", help="Extract data for specific domain only")
    parser.add_argument(
        "-s", "--sources", help="List of sources to extract from", nargs="*"
    )
    parser.add_argument(
        "--config", default="datasources.yaml", help="Path to datasources config file"
    )
    parser.add_argument(
        "-e", "--explain", help="Explain the chosen data sources", action="store_true"
    )
    parser.add_argument("--apis", help="APIs to be explained", nargs="*")

    return parser.parse_args()


def extract_data(
    config: DataSourceModel,
    models: list[DomainModel],
) -> None:

    for model in models:

        try:

            extractor = create_extractor(config, model)
            extractor.execute()

        except Exception as e:

            logger.exception(f"Error extracting data from {model.name}: {str(e)}")


def main():

    args = parse_args()
    needs_explanation = args.explain

    try:
        config = load_config(args.config, response_model=DataSourceModel)
    except Exception as e:
        logger.info(f"Error loading config file: {str(e)}")
        sys.exit(1)

    # build the arguments
    api_selection = (
        [
            v
            for k, v in config.APIs.items()
            if k.casefold()
            in [a.casefold() for a in args.apis]  # case insensitive comparison
        ]
        if args.apis
        else []
    )

    all_models: list[DomainModel] = []

    if args.domain:
        all_models = list(config.get_models(args.domain).values())
    elif args.sources:
        all_models = list(
            {k: v for k, v in config.get_models().items() if k in args.sources}.values()
        )
    else:
        all_models = list(config.get_models().values())  # get all models

    if needs_explanation:
        print(explain_source(config, apis=api_selection, models=all_models))
    else:
        extract_data(config, all_models)


if __name__ == "__main__":
    main()
