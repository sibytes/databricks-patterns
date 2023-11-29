from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
import logging
from pyspark.sql import SparkSession
from ._load_type_factory import get_load_type

class Table():

  _SCHEMA_PATH = "./../schema"
  _SQL_PATH = "./../sql"

  def __init__(
      self, 
      name:str,
      load_type:str,
      filename:str, 
      stage_table:str,
      stage_description: str,
      ):
    self._logger = logging.getLogger(self.__class__.__name__)

    # custom table properties
    self.name = name
    self.filename = filename
    self.stage_description = stage_description

    self.schema:StructType = self._load_schema(name = self.name)
    # get the extract and load functions
    load_type_config = get_load_type(load_type)
    self._extract = load_type_config["extract"]
    self._load = load_type_config["load"]

  def stage_into(self, spark:SparkSession):
    stage_db = "raw_cp_header_footer"
    self._logger.info(f"creating stage table `{stage_db}`.`{self.name}`")
    sql = f"""
      create schema if not exists `{stage_db}`
    """
    self._logger.debug(sql)
    spark.sql(sql)

    sql = f"""
      create table if not exists `{stage_db}`.`{self.name}`
      comment '{self.stage_description}'
      -- TBLPROPERTIES (<table-properties>);
    """
    self._logger.debug(sql)
    spark.sql(sql)

    path = f"/Volumes/development/landing/header_footer/{self.filename}/*/{self.filename}-*.csv"
    self._logger.info(f"copy into {path} into `{stage_db}`.`{self.name}`")
    sql = f"""
      copy into `{stage_db}`.`{self.name}`
      FROM '{path}'
      FILEFORMAT = CSV
      FORMAT_OPTIONS ('mergeSchema' = 'true')
      COPY_OPTIONS ('mergeSchema' = 'true');
    """
    self._logger.debug(sql)
    spark.sql(sql)

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
# add any custom properties needed, but you must add the to class constructor also.
def tables():

  config = {
    "customer_details_1": {
      "stage_description": "my description",
      "filename": "customer_details_1",
      "class": Table
    },
    "customer_details_2": {
      "stage_description": "my description",
      "filename": "customer_details_2",
      "class": Table
    },
    "customer_preferences": {
      "stage_description": "my description",
      "filename": "customer_preferences",
      "class": Table
    }
  }

  return config