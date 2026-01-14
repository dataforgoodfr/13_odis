import logging
import logging.config
import os
from pathlib import Path
from common.config import load_config

# Using classic logger for now. TODO : use python-json-logger
# LOG_CONFIG_PATH = "common/utils/logging_config.yml"
LOG_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'logging_config.yml')

# Create log directory if it doesn't exist
log_dir = Path("log")
log_dir.mkdir(exist_ok=True)

LOG_CONFIG = load_config(LOG_CONFIG_PATH)

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("main")