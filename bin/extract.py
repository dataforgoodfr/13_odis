#!/usr/bin/env python3
import argparse
import sys
import os
from pprint import PrettyPrinter
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils.source_extractors import FileExtractor, JsonExtractor
from common.config import load_config

pp = PrettyPrinter(indent=4)

def explain_source(config, apis=None, domain=None, models=None, default_all=False):

    all_apis = config.get('APIs')
    apis_list = list(all_apis.keys())

    all_domains = config.get('domains')
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
    print(f"\n\n================= API explanations =================\n")
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
    print(f"\n\n================= Domain & Models explanations =================\n")
    print(f"Available Domains from config : {domains_list}\n")
    print(f"Available Models from config : {models_list}\n")

    models_to_explain = []
    
    if models:
        models_to_explain = [model for model in all_models if model[1] in models]
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


def parse_args():

    parser = argparse.ArgumentParser(description='Extract data from configured sources')
    parser.add_argument('-d', '--domain', help='Extract data for specific domain only')
    parser.add_argument('-s','--sources', help='List of sources to extract from', nargs='*')
    parser.add_argument('--config', default='datasources.yaml', help='Path to datasources config file')
    parser.add_argument('-e','--explain', help='Explain the chosen data sources', action='store_true')
    parser.add_argument('--apis', help='APIs to be explained', nargs='*')
    
    return parser.parse_args()

def get_source_extractor(source_type):
    # This can be expanded based on supported source types
    extractors = {
        'file_extractor': FileExtractor,
        'json_extractor': JsonExtractor
    }
    if source_type not in extractors.keys():
        print(f"No extractor implemented for source type: {source_type}")

    return extractors.get(source_type)

def extract_data(config, domain=None, sources=None):

    if config['domains'][domain]:
        all_sources = config['domains'][domain].keys()
        if not all_sources:
            print(f"No sources found for domain: {domain}")
            return
    
    # Set the source list : if none was given in input, take all for given domain
    sources_list = sources if sources else all_sources
    
    for source_name in sources_list:

        print(f"Extracting source: {domain}/{source_name}")

        source = config['domains'][domain][source_name]
        
        source_type = source['type']
        if not source_type:
            print(f"Source type not specified for {source_name}, skipping...")
            continue
            
        extractor_class = get_source_extractor(source_type)

        if extractor_class:
            extractor = extractor_class(config, domain)      
            # try:
            filepath = extractor.download(domain, source_name)
            print(f"Data extracted from {source_name} and saved to {filepath}")
            # except Exception as e:
            #     print(f"Error extracting data from {source_name}: {str(e)}")

def main():
    args = parse_args()
    needs_explanation = args.explain
    
    try:
        config = load_config(args.config)
   
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        sys.exit(1)
    
    if needs_explanation:
        explain_source(config, domain = args.domain, apis = args.apis, models = args.sources)

    else:
        extract_data(config, args.domain, args.sources)

if __name__ == '__main__':
    main()
