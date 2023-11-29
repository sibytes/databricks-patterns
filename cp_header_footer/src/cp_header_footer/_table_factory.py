import os
from ._logging import configure_logging
import logging
from ._table import tables

configure_logging()

def get_table(table: str, load_type: str):

  try:
    table_metadata = tables()[table]
  except KeyError as e:
    msg = f"table {table} not found in factory metadata"
    raise Exception(msg) from e
  
  try:
    table_cls = table_metadata["class"]
    del table_metadata["class"]
    table_metadata["name"] = table
    table_metadata["load_type"] = load_type

  except KeyError as e:
    msg = f"table {table} has no class defined in the factory metadata it must have one."
    raise Exception(msg) from e

  return table_cls(**table_metadata)


def get_load_type(load_type: str) -> dict:

  try:
    load_type_config = load_types[load_type]
  except KeyError as e:
    raise e

  return load_type_config
