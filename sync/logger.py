import logging

LOG_LEVEL = logging.DEBUG

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
