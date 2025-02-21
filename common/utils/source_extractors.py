from abc import ABC, abstractmethod
import requests
from pathlib import Path

class SourceExtractor(ABC):
    """Abstract class defining a datasource extractor.
    Only the 'extract' method is mandatory, which is responsible
    for pulling the data and storing it in a local file.
    """
    @abstractmethod
    def download(self, domain: str, source_config):
        pass

class MetadonneesApiExtractor(SourceExtractor):
    """Extract data from Metadonnees API"""
    def download(self, domain: str, source_config):
        headers = {
            'Accept': 'application/json',
        }

        # Make request to API
        response = requests.get(source_config['url'], headers=headers)
        response.raise_for_status()

        # Create data directory if it doesn't exist
        data_dir = Path(f"data/imports/{domain}")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename from source name
        filename = f"{source_config['name']}.json"
        filepath = data_dir / filename

        # Write response content to file
        with open(filepath, 'wb') as f:
            f.write(response.content)

        return str(filepath)

