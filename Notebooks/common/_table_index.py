from pydantic import BaseModel, Field
from typing import Union, List, Any, Dict
from enum import Enum
import fnmatch


_INDEX_WILDCARD = "*"


class StageType(str, Enum):
    landing = "landing"
    raw = "raw"
    base = "base"


class BaseTable(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    stage: StageType = Field(...)
    database: str = Field(...)
    name: str = Field(...)
    id: Union[str, List[str]] = Field(default=[])


class Table(BaseTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    depends_on: List[str] = Field(default=[])


class TableMapping(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    destination: Table = Field(...)
    source: Union[Dict[str, Table], Table] = Field(...)


class Tables(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._load_index()

    table_data: dict = Field(...)
    tables_index: Dict[str, Table] = Field(default={})

    @classmethod
    def get_index(
        cls,
        stage: Union[StageType, str] = _INDEX_WILDCARD,
        table=_INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
    ):
        return f"{stage.name}.{database}.{table}"

    def _load_index(self):
        for stage in StageType:
            stage_data = self.table_data.get(stage.value)
            if stage_data:
                for database, tables in stage_data.items():
                    for table, table_details in tables.items():
                        # flatten the config structure for a table
                        if not table_details:
                            table_details = {}
                        table_details["name"] = table
                        table_details["database"] = database
                        table_details["stage"] = stage

                        # create a table object
                        table = Table(**table_details)

                        # index the table object
                        index = f"{stage.value}.{database}.{table.name}"
                        self.tables_index[index] = table

    def lookup_table(self, index: str, first_match: bool = True):
        matches = fnmatch.filter(list(self.tables_index.keys()), index)

        if not matches:
            raise Exception(f"index {index} not found in tables_index")

        if first_match:
            matches = matches[0]
            table = self.tables_index[matches]
            return table
        else:
            tables = [self.tables_index[i] for i in matches]
            return tables

    def get_table_mapping(
        self,
        stage: StageType,
        table=_INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        index: str = None,
    ):
        if not index:
            index = Tables.get_index(stage, table, database)

        destination = self.lookup_table(index=index, first_match=True)
        source = {}

        try:
            for index in destination.depends_on:
                table = self.lookup_table(index=index, first_match=True)
                source[table.name] = table
        except Exception as e:
            raise Exception("Error looking up dependencies for table {}") from e

        if len(list(source.values())) == 1:
            source = list(source.values())[0]

        return TableMapping(source=source, destination=destination)
