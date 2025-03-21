#!/usr/bin/env python3
import argparse
import sys
import os
import datetime
import json
import jmespath
from importlib import import_module
from pprint import PrettyPrinter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))    

from common.config import load_config
from common.utils.logging_odis import logger
from common.utils.file_handler import FileHandler
from common.data_source_model import DataProcessLog

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

def get_connector_class(connector_name: str, module_name: str):
    
    # imports the Extractor class from the specified module
    source_module = import_module(f"common.utils.{module_name}")
    imported_class = getattr(source_module, connector_name)
    logger.debug(f"Imported class: {imported_class}")
    return imported_class

def explain(apis:list[str] = None, domain:str = None, sources:list[str] = None, **params):

    all_apis = CONFIG.get('APIs')
    apis_list = list(all_apis.keys())

    all_domains = CONFIG.get('domains')
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
    print("\n\n================= API explanations =================\n")
    print(f"Available APIs from config : {apis_list}")
    api_selection = [(api,all_apis.get(api)) for api in apis] if apis else []
    for api_name,api_conf in api_selection:
        print(f"\n~~~~~~ {api_name} ~~~~~~")
        print(f"{json.dumps(api_conf,indent=4)}")
        used_in = [
            (model[0],model[1]) for model in all_models if model[2]['API'] == api_name 
        ]
        print(f"This API is used in the following models : {pp.pformat(used_in)}\n")


    # Pretty print information about the domains and source models
    print("\n\n================= Domain & Models explanations =================\n")
    print(f"Available Domains from config : {domains_list}\n")
    print(f"Available Models from config : {models_list}\n")

    models_to_explain = []
    
    if sources:
        models_to_explain = [model for model in all_models if model[1] in sources]
    else:
        if domain:
            domain_to_explain = all_domains.get(domain)
            models_to_explain = [model for model in all_models if model[1] in domain_to_explain.keys()]
            print("------------------------------------------------")
            print(f"Current selected domain: {domain}")
            print("------------------------------------------------\n")
        else:
            models_to_explain = []

    for domain_name, model_name, model_conf in models_to_explain:
        print(f"\nSOURCE MODEL: {domain_name} :: {model_name}")
        print(f"{json.dumps(model_conf,indent=4)}")

def extract(domain:str = None, sources:list[str] = None, **params):

    # Set the source list : if none was given in input, take all for given domain
    sources_list = sources if sources else SOURCES_INDEX.get(domain)
    if not sources_list:
        logger.info(f"No domain found in cdatasources.yaml for : {domain}, skipping...")
        return
    
    for source_name in sources_list:
        
        source_namespace = f"{domain}.{source_name}"
        logger.info(f"Extracting source: {source_namespace}")

        source = jmespath.search(source_namespace, DOMAINS)
        source_type = source.get('type') if source else None
        if not source_type:
            logger.info(f"Source type not specified for {source_name}, skipping...")
            continue
        
        # initialize process log
        process_log = DataProcessLog(domain, source_name, 'extract', source_config = source)
        
        # Import the Extractor class specified in the source config
        extractor_class = get_connector_class(source_type, 'source_extractors')
            
        try:
            # Instantiate the Extractor
            extractor = extractor_class(CONFIG,domain)

            # All extractors 'download' must yield iterable results
            extract_generator = extractor.download(domain, source_name)
    
            # iterate through the extraction generator
            for _, page_number, is_last, filepath in extract_generator:
                process_log.update_pagelog(page_number, filepath = filepath, is_last = is_last, extracted = True)

            # save the process log locally
            fh.file_dump(domain, f"{source_name}_extract_log", payload = process_log.to_dict())
            
        except Exception as e:
            logger.exception(f"Error extracting data from {source_name}: {str(e)}")

def load(domain:str = None, sources:list[str] = None, **params):
    
    # Set the source list : if none was given in input, take all for given domain
    sources_list = sources if sources else SOURCES_INDEX[domain]

    for source_name in sources_list:

        source_namespace = f"{domain}.{source_name}"
        logger.info(f"Loading source: {source_namespace}")

        source = jmespath.search(source_namespace, DOMAINS)
        source_format = source.get('format') if source else None
        if not source_format:
            logger.info(f"Source format not specified for {source_name}, skipping...")
            continue

        # little trick to build the loader class name from format
        loader_name = f"{str.capitalize(source_format)}DataLoader"

        try:
            # read data extraction log provided by extractor
            data_index_name = f"{source_name}_extract_log"
            data_index = fh.json_load(domain = domain, source_name = data_index_name)

            # Load and update a new ProcessLog object from the extract log
            process_log = DataProcessLog.from_dict(data_index)
            process_log.last_runtime = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            process_log.operation = 'load'

            # Import the Loader module
            loader_class = get_connector_class(loader_name, 'data_loaders')
            
            # Instantiate the loader
            loader = loader_class()
            
            # (Re)-initializing the database bronze table
            table_name = f"{domain}_{source_name}"
            init_success = loader.init_jsontable(table_name)

            if init_success:
                # instantiate page-by-page load generator
                load_generator = loader.load_pagelogs(process_log)

                # iterate through generator and log progress
                for pageno, load_success in load_generator:                 
                    process_log.update_pagelog(pageno, bronze_loaded = load_success)
            
            else:
                logger.info(f"Could not initialize table {table_name}. Exiting.")
            
            # save the process log locally
            fh.file_dump(domain, f"{source_name}_load_log", payload = process_log.to_dict())
            
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
