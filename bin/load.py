#!/usr/bin/env python3
import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import load_config
from common.utils.data_loaders import JsonDataLoader
from dotenv import load_dotenv
load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(description='Load raw data in database bronze layer')
    parser.add_argument('--domain', help='Load data for specific domain only')
    parser.add_argument('--config', default='datasources.yaml', help='Path to datasources config file')
    return parser.parse_args()

def get_data_loader(source_format):
    data_loaders = {
        'json': JsonDataLoader,
    }
    return data_loaders.get(source_format)

def load_data(config, domain=None):

    if config['domains'][domain]:
        sources = config['domains'][domain]['sources']
        if not sources:
            print(f"No sources found for domain: {domain}")
            return
    
    for source in sources:
        source_name = source.get('name')

        print(f"Loading source: {domain}/{source_name}")
        
        source_format = source['format']
        if not source_format:
            print(f"Source format not specified for {source_name}, skipping...")
            continue
            
        data_loader_class = get_data_loader(source_format)
        if not data_loader_class:
            print(f"No extractor implemented for source type: {source_format}")
            continue

        try:
            data_loader = data_loader_class()
            data_loader.load(domain, source)
            print(f"Loaded: {domain}/{source_name}")
        except Exception as e:
            print(f"Error loading data from {source_name}: {str(e)}")

def main():
    args = parse_args()
    
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        sys.exit(1)
        
    load_data(config, args.domain)

if __name__ == '__main__':
    main()
