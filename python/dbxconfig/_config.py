import yaml
import os
from ._source import Source
from ._deltalake import DeltaLake
from ._timeslice import Timeslice
from pydantic import BaseModel, Field, PrivateAttr
from typing import Union
from ._table_index import Tables, StageType, Table, _INDEX_WILDCARD
from typing import Dict

# class StageTables(BaseModel):
#   source:str = Field(...)
#   alias:str = Field(default=None)
#   raw:str = Field(...)
#   base:str = Field(...)


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

    def __init__(self, timeslice: Timeslice, config_path: str = None):
        self.config = {}

        if not config_path:
            config_path = os.path.join(self._CONFIG_PATH, self._CONFIG_FILE)

        with open(config_path, "r", encoding=self._ENCODING) as f:
            self.config = yaml.safe_load(f)

        _tables_path = self.config["tables"]

        with open(_tables_path, "r", encoding=self._ENCODING) as f:
            self.config["tables"] = yaml.safe_load(f)

        self.tables = Tables(table_data=self.config["tables"])

    def get_table_mapping(
        self,
        timeslice: Timeslice,
        stage: StageType,
        table: str = _INDEX_WILDCARD,
        database: str = _INDEX_WILDCARD,
        index: str = None,
    ):
        table_mapping = self.tables.get_table_mapping(
            stage=stage, table=table, database=database, index=index
        )

        if isinstance(table_mapping.source, dict):
            for name, table_obj in table_mapping.source.items():
                stage_config = self._get_stage_config(table_obj, timeslice)
                if table_obj.stage == StageType.landing:
                    table_mapping.source[name] = Source(**stage_config)
                if table_obj.stage != StageType.landing:
                    table_mapping.source[name] = DeltaLake(**stage_config)

        elif isinstance(table_mapping.source, Table):
            stage_config = self._get_stage_config(table_mapping.source, timeslice)
            if table_mapping.source.stage == StageType.landing:
                table_mapping.source[name] = Source(**stage_config)
            if table_mapping.source.stage != StageType.landing:
                table_mapping.source[name] = DeltaLake(**stage_config)

        stage_config = self._get_stage_config(table_mapping.destination, timeslice)
        if table_mapping.destination.stage in (StageType.base, StageType.raw):
            table_mapping.destination = DeltaLake(**stage_config)
        else:
            raise Exception(
                f"Table mapping destinations must be {StageType.raw} ro {StageType.base}"
            )

        return table_mapping

    def _get_stage_config(self, table: Table, timeslice: Timeslice):
        stage_config = self.config[table.stage.name]
        stage_config[self._TIMESLICE] = timeslice
        stage_config[self._TABLE] = table.name

        return stage_config

    def link_checkpoint(
        self,
        source: Union[Source, DeltaLake],
        destination: DeltaLake,
        checkpoint_name: str = None,
    ):
        if not checkpoint_name:
            checkpoint_name = f"{source.database}.{source.table}-{destination.database}.{destination.table}"

        source.checkpoint = checkpoint_name
        source._render()
        destination.checkpoint = checkpoint_name
        destination._render()
