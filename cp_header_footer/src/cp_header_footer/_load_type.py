from pyspark.sql import DataFrame
import logging

def batch_extract(table) -> DataFrame:
  logger = logging.getLogger(__name__)
  logger.info(f"batch_extract {table.filename}")
  df = None
  return df


def batch_load(table) -> DataFrame:
  logger = logging.getLogger(__name__)
  logger.info(f"batch_load {table.name}")
  df = None
  return df

# register functions here and map them to a load type
# must be last because it's a module of functions
def load_types():
  return {
    "batch": {
      "extract": batch_extract,
      "load": batch_load
    }
  }