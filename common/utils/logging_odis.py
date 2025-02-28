import logging
import logging.config
from common.config import load_config

# Using classic logger for now. TODO : use python-json-logger
LOG_CONFIG_PATH = 'common/utils/logging_config.yml'
LOG_CONFIG = load_config(LOG_CONFIG_PATH)

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('main')


