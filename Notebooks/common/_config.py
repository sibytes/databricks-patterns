import yaml
import os
from ._source import Source
from ._deltalake import DeltaLake
from ._timeslice import Timeslice
from pydantic import BaseModel, Field, PrivateAttr
from typing import Union
class StageTables(BaseModel):
  source:str = Field(...)
  alias:str = Field(default=None)
  raw:str = Field(...)
  base:str = Field(...)
  

class Config:

  _CONFIG_PATH = "../Config/"
  _CONFIG_FILE = "config.yaml"
  _ENCODING = "utf-8"
  _BRONZE = "raw"
  _SILVER = "base"
  _SOURCE = "source"
  _TABLES = "tables"
  _TIMESLICE = "timeslice"
  _TABLE = "destination_table"
  _SOURCE_TABLE = "source_table"

  def __init__(self, timeslice:Timeslice, config_path:str=None):

    self.config = {}

    if not config_path:
      config_path = os.path.join(self._CONFIG_PATH, self._CONFIG_FILE)

    with open(config_path, "r", encoding=self._ENCODING) as f:
      self.config = yaml.safe_load(f)

    self.timeslice = timeslice
    self.tables = []

    for t in self.config[self._TABLES]:
      source = next(iter(t))
      st = StageTables(source=source, raw=t[source][self._BRONZE], base=t[source][self._SILVER])
      self.tables.append(st)

  def _inject_keywords(self, table:str, timeslice:Timeslice, stage:str):
    stage = self.config[stage]
    stage[self._TIMESLICE] = timeslice
    stage[self._TABLE] = table
    return stage

  def get_source(self, table:str):
    source = self._inject_keywords(table, self.timeslice, self._SOURCE)
    source = Source(**source)
    return source

  def get_raw(self, table:str):
    raw = self._inject_keywords(table, self.timeslice, self._BRONZE)
    raw = DeltaLake(**raw)
    return raw
  
  def get_base(self, table:str):
    base = self._inject_keywords(table, self.timeslice, self._SILVER)
    base = DeltaLake(**base)
    return base

  def link_checkpoint(
    self, 
    source:Union[Source, DeltaLake], 
    destination:DeltaLake, 
    checkpoint_name:str=None
  ):
    if not checkpoint_name:
      checkpoint_name = f"{source.database}.{source.table}-{destination.database}.{destination.table}"

    source.checkpoint = checkpoint_name
    source._render()
    destination.checkpoint = checkpoint_name
    destination._render()


  