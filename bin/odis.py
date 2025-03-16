#!/usr/bin/env python3
import argparse

import sys
import os
from pprint import PrettyPrinter

import datetime

import json
import os
import sys
from importlib import import_module
from io import StringIO
from pprint import PrettyPrinter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import load_config
from common.data_source_model import DataSourceModel, DomainModel
from common.utils.file_handler import FileHandler

from common.data_source_model import DataProcessLog

from common.utils.logging_odis import logger


pp = PrettyPrinter(indent=4)
fh = FileHandler()


# define globals
DEFAULT_CONFIGFILE = 'datasources.yaml'
CONFIG: dict
DOMAINS: dict
SOURCES_INDEX: dict


def parse_args():
    """Defines the Argument Parser and returns parsed args"""

    # Define Argument Parser
    parser = argparse.ArgumentParser(description='Extract data from configured sources')
    parser.add_argument('operation', action='store', help='Action to be taken : extract, load, explain')
    parser.add_argument('-d', '--domain', help='Extract data for specific domain only')
    parser.add_argument('-s','--sources', help='List of sources to extract from', nargs='*')
    parser.add_argument('-f','--file', help='Path to an input file')
    parser.add_argument('--config', default=DEFAULT_CONFIGFILE, help='Path to datasources config file')
    parser.add_argument('--apis', help='APIs to be explained', nargs='*')
    
    return parser.parse_args()




def explain(
    config: DataSourceModel,
    apis: list[str] = None,
    domain: str = None,
    models: list[str] = None,
    default_all: bool = False,
) -> str:

    output = StringIO()


    all_apis = config.get("APIs")
    apis_list = list(all_apis.keys())

    all_domains = config.get("domains")

    domains_list = list(all_domains.keys())

    models_list = []
    all_models = []
    for domain_name, domain_value in all_domains.items():
        # all_models.extend(domain_value.items())
        for name, conf in domain_value.items():
            models_list.append(name)
            full_model_info = [domain_name, name, conf]
            all_models.append(full_model_info)


    # Pretty print information about the APIs
    output.write("\n\n================= API explanations =================\n")
    output.write(f"Available APIs from config : {list(config.APIs.keys())}\n")

    api_selection = (
        [
            v
            for k, v in config.APIs.items()
            if k.casefold()
            in [a.casefold() for a in apis]  # case insensitive comparison
        ]
        if apis
        else []
    )

    for a in api_selection:
        output.write(f"\n~~~~~~ {a.name} ~~~~~~")
        output.write(f"{a.model_dump_json(indent=4)}\n")

        used_in = config.get_api_domains(a.name)
        output.write(
            f"This API is used in the following models : {pp.pformat(used_in)}\n"
        )

    # Pretty print information about the domains and source models
    output.write(
        "\n\n================= Domain & Models explanations =================\n"
    )
    output.write(f"Available Domains from config : {list(config.domains.keys())}\n")
    output.write(f"Available Models from config : {list(config.get_models().keys())}\n")

    models_to_explain: dict[str, DomainModel] = config.get_models()

    
    if sources:
        models_to_explain = [model for model in all_models if model[1] in sources]


    if models:

        models_to_explain = [model for model in all_models if model[1] in models]


        # explain only the selected models
        models_to_explain = {
            k: v for k, v in config.get_models().items() if k in models
        }

    else:
        if domain:
            # explain all models for the selected domain
            models_to_explain = config.get_models(domain=domain)
            output.write("------------------------------------------------")
            output.write(f"Current selected domain: {domain}")
            output.write("------------------------------------------------\n")

    for k, v in models_to_explain.items():
        output.write(f"\nSOURCE MODEL: {k}\n")
        output.write(f"{v.model_dump_json(indent=4)}\n")

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


def extract_data(config, domain=None, sources=None):

    if config["domains"][domain]:
        all_sources = config["domains"][domain].keys()
        if not all_sources:
            print(f"No sources found for domain: {domain}")
            return


    # Set the source list : if none was given in input, take all for given domain
    sources_list = sources if sources else SOURCES_INDEX[domain]


    # start_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    

    start_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


    for source_name in sources_list:
        
        # initialize process log
        processlog = DataProcessLog(domain, source_name, 'extract')
        
        print(f"Extracting source: {domain}/{source_name}")


        source = DOMAINS[domain][source_name]
        
        source_type = source['type']

        source = config["domains"][domain][source_name]

        source_type = source["type"]

        if not source_type:
            logger.info(f"Source type not specified for {source_name}, skipping...")
            continue

        # Import the Extractor class specified in the source config
        source_type = source.get("type")
        extractor_class = get_connector_class(source_type, "source_extractors")

        try:
            # Instantiate the Extractor and execute download

            extractor = extractor_class(CONFIG,domain)

            # All extractors 'download' must yield iterable results
            extract_generator = extractor.download(domain, source_name)
    

            extractor = extractor_class(config, domain)

            # All extractors 'download' must yield iterable results
            extract_generator = extractor.download(domain, source_name)

            file_dumps = []
            last_page_downloaded = 0
            complete = False


            # iterate through the extraction generator
            for _, page_number, is_last, filepath in extract_generator:
                processlog.add_pagelog(page_number, filepath = filepath, is_last = is_last)


            # save the process log locally
            fh.file_dump(domain, f"{source_name}_extract_log", payload = processlog.to_dict())
            
        except Exception as e:
            logger.exception(f"Error extracting data from {source_name}: {str(e)}")


def load(domain:str = None, sources:list[str] = None, **params):
    
    # Set the source list : if none was given in input, take all for given domain
    sources_list = sources if sources else SOURCES_INDEX[domain]

                last_page_downloaded = page_number
                complete = is_last
                filedump_info = {"page": page_number, "filepath": filepath}
                file_dumps.append(filedump_info)

            logger.debug(f"LAST PAGE DOWNLOADED: {last_page_downloaded}")

            # dump the log of the full extract iteration
            extract_metadata = {
                "domain": domain,
                "source": source_name,
                "last_run_time": start_time,
                "last_page_downloaded": last_page_downloaded,
                "successfully_completed": complete,
                "file_dumps": file_dumps,
            }
            fh.file_dump(domain, f"{source_name}_metadata", payload=extract_metadata)

        except Exception as e:
            logger.exception(f"Error extracting data from {source_name}: {str(e)}")


def main():

    args = parse_args()
    needs_explanation = args.explain

    try:
        config = load_config(args.config)

    except Exception as e:
        logger.info(f"Error loading config file: {str(e)}")
        sys.exit(1)

    if needs_explanation:
        config = load_config(args.config, response_model=DataSourceModel)
        explanations = explain_source(
            config, domain=args.domain, apis=args.apis, models=args.sources
        )
        print(explanations)


    for source_name in sources_list:

        print(f"Loading source: {domain}/{source_name}")

        source = DOMAINS[domain][source_name]
        
        source_format = source['format']
        # little trick to build the loader class name from format
        loader_name = f"{str.capitalize(source_format)}DataLoader"
        
        try: 
            # Import the Loader module
            data_loader_class = get_connector_class(loader_name, 'data_loaders')

            # Instantiate the loader and load files from local data folder
            data_loader = data_loader_class()
            data_loader.load(domain, source_name)
        
        except Exception as e:
            logger.exception(f"Issue in loading data : {e}")


if __name__ == '__main__':
    
    try:
        args = parse_args()

        # load config file
        config_name = args.config
        CONFIG = load_config(config_name)
        
        # build useful dicts 
        DOMAINS = CONFIG.get('domains')
        SOURCES_INDEX = {}
        for domain_name in DOMAINS.keys():
            SOURCES_INDEX[domain_name] = [name for name in DOMAINS[domain_name].keys()]

        # load the function to be executed
        operation = locals()[args.operation]
   
    except Exception as e:
        logger.exception(f"Error preparing CLI arguments: {e}")
        sys.exit(1)
    
    # execute operation with args
    operation(**vars(args))


if __name__ == "__main__":
    main()

