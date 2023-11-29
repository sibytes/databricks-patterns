from pyspark.sql import DataFrame
import logging

def batch_extract(table) -> DataFrame:
  logger = logging.getLogger(__name__)
  logger.info(f"batch_extract {table.filename}")
  return None


def batch_load(table) -> DataFrame:
  logger = logging.getLogger(__name__)
  logger.info(f"batch_load {table.name}")
  return None

# register functions here and map them to a load type
# must be last because it's a module of functions
def load_types():
  return {
    "batch": {
      "extract": batch_extract,
      "load": batch_load
    }
  }