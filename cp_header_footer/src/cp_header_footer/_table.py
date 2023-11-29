from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
import yaml
import logging
from ._load_type_factory import get_load_type

class Table():

  _SCHEMA_PATH = "./../schema"
  _SQL_PATH = "./../sql"

  def __init__(self, name:str, filename:str, load_type:str):
    self._logger = logging.getLogger(self.__class__.__name__)
    self.name = name
    self.filename = filename
    self.schema:StructType = self._load_schema(name = self.name)

    # get the extract and load functions
    load_type_config = get_load_type(load_type)
    self._extract = load_type_config["extract"]
    self._load = load_type_config["load"]

  def stage_into(self):
    pass

  def extract(self):
    df = self._extract(self)
    self._logger.info(f"extracted {self.filename}")
    return df

  def transform(self, df:DataFrame):
    self._logger.info("transform")
    return df

  def load(self, df:DataFrame):
    df = self._load(self)
    self._logger.info(f"loaded {self.name}")
    return df

  def _load_schema(self, name:str):
    self._logger.info("load schema")


# register tables here and map them to a table Class
# this register must be last so that the classes are loaded 1st
def tables():
  return {
    "customer_details_1": {
      "stage_table": "raw_customer_details_1",
      "filename": "customer_details_1",
      "class": Table
    },
    "customer_details_2": {
      "stage_table": "raw_customer_details_2",
      "filename": "customer_details_2",
      "class": Table
    },
    "customer_preferences": {
      "stage_table": "raw_customer_preferences",
      "filename": "customer_preferences",
      "class": Table
    }
  }