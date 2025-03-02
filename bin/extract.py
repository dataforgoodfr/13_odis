#!/usr/bin/env python3
import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils.source_extractors import FileExtractor, JsonExtractor
from common.config import load_config

def parse_args():
    parser = argparse.ArgumentParser(description='Extract data from configured sources')
    parser.add_argument('-d', '--domain', help='Extract data for specific domain only')
    parser.add_argument('-s','--sources', help='List of sources to extract from', nargs='+')
    parser.add_argument('--config', default='datasources.yaml', help='Path to datasources config file')
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
    
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        sys.exit(1)
        
    extract_data(config, args.domain, args.sources)

if __name__ == '__main__':
    main()
