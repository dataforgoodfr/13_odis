#!/usr/bin/env python3
import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils.source_extractors import MetadonneesApiExtractor
from common.utils.logging_odis import logger
from common.config import load_config

def parse_args():
    parser = argparse.ArgumentParser(description='Extract data from configured sources')
    parser.add_argument('--domain', help='Extract data for specific domain only')
    parser.add_argument('--config', default='datasources.yaml', help='Path to datasources config file')
    return parser.parse_args()

def get_source_extractor(source_type):
    # This can be expanded based on supported source types
    extractors = {
        'metadonnees_api': MetadonneesApiExtractor,
    }
    return extractors.get(source_type)

def extract_data(config, domain=None):

    if config['domains'][domain]:
        sources = config['domains'][domain]['sources']
        if not sources:
            logger.info(f"No sources found for domain: {domain}")
            return
    
    for source in sources:
        source_name = source.get('name')

        logger.info(f"Extracting source: {domain}/{source_name}")
        logger.debug("BLEEHJHH")
        
        source_type = source['type']
        if not source_type:
            logger.info(f"Source type not specified for {source_name}, skipping...")
            continue
            
        extractor_class = get_source_extractor(source_type)
        if not extractor_class:
            logger.info(f"No extractor implemented for source type: {source_type}")
            continue
            
        try:
            extractor = extractor_class()
            filepath = extractor.download(domain, source)
            logger.info(f"Data extracted from {source_name} and saved to {filepath}")
        except Exception as e:
            logger.exception(f"Error extracting data from {source_name}: {str(e)}")

def main():
    args = parse_args()
    
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.info(f"Error loading config file: {str(e)}")
        sys.exit(1)
        
    extract_data(config, args.domain)

if __name__ == '__main__':
    main()
