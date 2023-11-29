from ._load_type import *

def get_load_type(load_type: str) -> dict:

  try:
    load_type_config = load_types()[load_type]
  except KeyError as e:
    raise e

  return load_type_config
