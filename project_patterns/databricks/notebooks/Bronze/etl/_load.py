from yetl import Read, DeltaLake
from databricks.sdk.runtime import spark
import hashlib

def hash_value(value:str):

  hash_object = hashlib.sha224(f"{value}".encode('utf-8'))
  hex_dig = hash_object.hexdigest()
  return hex_dig

def load(
  process_id:int,
  source:Read,
  destination:DeltaLake,
  await_termination:bool = True
):

  stream = (spark.readStream
    .schema(source.spark_schema)
    .format(source.format)
    .options(**source.options)
    .load(source.path)
  )

  src_cols = [c for c in stream.columns if not c.startswith("_")]
  sys_cols = [c for c in stream.columns if c.startswith("_")]

  columns = src_cols + sys_cols + [
    "if(_corrupt_record is null, true, false) as _is_valid",
    f"cast({process_id} as long) as _process_id",
    "current_timestamp() as _load_date",
    "_metadata"
  ]
  print("\n".join(columns))

  stream_data:StreamingQuery = (stream
    .selectExpr(*columns)
    .writeStream
    .options(**destination.options)
    .trigger(availableNow=True)
    .toTable(f"`{destination.database}`.`{destination.table}`")
  )

  
  if await_termination:
    stream_data.awaitTermination()