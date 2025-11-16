import logging
import sys

# Create logger
logger = logging.getLogger("etl_backend")
logger.setLevel(logging.INFO)

# Handler for stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Log format
fmt = "[%(asctime)s] [%(levelname)s] %(message)s"
handler.setFormatter(logging.Formatter(fmt))

logger.addHandler(handler)
