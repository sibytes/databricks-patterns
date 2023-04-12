import logging
from pydantic import BaseModel, Field, PrivateAttr
from ._utils import JinjaVariables, render_jinja, get_ddl, load_schema
from typing import Any, Dict, List, Union
from datetime import datetime
from ._timeslice import Timeslice
from enum import Enum
import os
from pyspark.sql.types import StructType
import logging
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame

class TriggerType(Enum):
  File = "file"

class Source(BaseModel):

  _OPTION_CF_SCHEMA_HINTS = "cloudFiles.schemaHints"

  def __init__(self, **data: Any) -> None:
    super().__init__(**data)
    self._logger = logging.getLogger(self.__class__.__name__)
    self._render()
    self.path = os.path.join(self.root, self.filename)

  _logger: Any = PrivateAttr(default=None)
  _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
  trigger:str = Field(default=None)
  trigger_type:TriggerType = Field(default=None)
  database:str = Field(...)
  destination_table:str = Field(...)
  table:str = Field(...)
  container:str = Field(...)
  root:str = Field(...)
  filename:str = Field(...)
  filename_date_format:str = Field(...)
  path_date_format:str = Field(...)
  options:dict = Field(...)
  timeslice:Timeslice = Field(...)
  format:str = Field(...)
  path:str = Field(default=None)
  spark_schema:StructType = Field(default=None)
  ddl:List[str] = Field(default=None)
  headerless_ddl:List[str] = Field(default=None)
  checkpoint:str = Field(default=None)

  def _render(self):
    self._replacements = {
      JinjaVariables.FILENAME_DATE_FORMAT: self.timeslice.strftime(
        self.filename_date_format
      ),
      JinjaVariables.PATH_DATE_FORMAT: self.timeslice.strftime(
        self.path_date_format
      ),
      JinjaVariables.TABLE: self.destination_table,
      JinjaVariables.DATABASE: self.database,
      JinjaVariables.CONTAINER: self.container,
      JinjaVariables.CHECKPOINT: self.checkpoint
    }

    self.root = render_jinja(self.root, self._replacements)
    self.filename = render_jinja(self.filename, self._replacements)
    self.database = render_jinja(self.database, self._replacements)
    self.table = render_jinja(self.table, self._replacements)
    self.trigger = render_jinja(self.trigger, self._replacements)

    if self.options:
      for option, value in self.options.items():
        self.options[option] = render_jinja(value, self._replacements)

    path = self.options.get(self._OPTION_CF_SCHEMA_HINTS, None)
    if path and "/" in path:
      self._load_schema(path)

      if self.options.get("header"):
        self.options[self._OPTION_CF_SCHEMA_HINTS] = ", ".join(self.ddl)
      else:
        self.options[self._OPTION_CF_SCHEMA_HINTS] = ", ".join(self.headerless_ddl)

  def _load_schema(self, path:str):
    if not self.spark_schema:
      self.spark_schema = load_schema(path)
    if not self.ddl:
      self.ddl = get_ddl(self.spark_schema, header=True)
    if not self.headerless_ddl:
      self.headerless_ddl = get_ddl(self.spark_schema, header=False)

  def rename_headerless(self, df:Union[StreamingQuery,DataFrame]):

    columns = [c for c in df.columns if c not in ['_rescued_data']]
    columns_cnt = len(columns)
    ddls = len(self.ddl)
    if columns_cnt != ddls:
      raise Exception(f"Headless files with schema hints must have a fully hinted schema since it must work positionally. Datasets!=dll({columns_cnt}!={ddls}")

    for i, c in enumerate(columns):
      from_name = f"_c{i}"
      to_name = self.ddl[i].split(" ")[0].strip()
      logging.info(f"rename {from_name} to {to_name}")
      df:Union[StreamingQuery,DataFrame] = df.withColumnRenamed(from_name, to_name)

    return df

  class Config:
    arbitrary_types_allowed = True



