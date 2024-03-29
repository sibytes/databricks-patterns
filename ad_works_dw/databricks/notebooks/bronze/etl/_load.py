from yetl import Read, DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql.streaming import StreamingQuery
import hashlib
from pyspark.sql import DataFrame
from typing import Union
from pyspark.sql import functions as fn
import logging


def hash_value(value: str):
    hash_object = hashlib.sha224(f"{value}".encode("utf-8"))
    hex_dig = hash_object.hexdigest()
    return hex_dig

def z_order_by(
    destination: DeltaLake
):
    _logger = logging.getLogger(__name__)
    _z_order_by = destination.z_order_by
    if isinstance(_z_order_by, list):
      _z_order_by = ",".join(destination.z_order_by)
    print("Optimizing")
    sql = f"""
        OPTIMIZE `{catalog.database}`.`{destination.database}`.`{destination.table}`
        ZORDER BY ({_z_order_by})
    """
    _logger.info(sql)
    spark.sql(sql)


def drop_if_already_loaded(df:Union[DataFrame, StreamingQuery], source:Read):
    already_loaded = spark.sql(f"""
      select struct(file_path, file_name, file_size, file_modification_time) as _metadata_loaded
      from {source.catalog}.control_ad_works_lt.raw_audit
      where source_table = '{source.table}'                   
    """)
    match_on_metadata = [
        fn.col("_metadata.file_path").alias("file_path"),
        fn.col("_metadata.file_name").alias("file_name"),
        fn.col("_metadata.file_size").alias("file_size"),
        fn.col("_metadata.file_modification_time").alias("file_modification_time")
    ]
    df = df.withColumn("_metadata_loading", fn.struct(*match_on_metadata))

    df = df.join(already_loaded, already_loaded._metadata_loaded == df._metadata_loading ,"left")
    df = df.where(df._metadata_loaded.isNull())
    df = df.drop(*["_metadata_loading", "_metadata_loaded"])
    return df



def stream_load(
    process_id: int,
    source: Read,
    destination: DeltaLake,
    drop_already_loaded:bool = True
):
    stream = (
        spark.readStream.schema(source.spark_schema)
        .format(source.format)
        .options(**source.options)
        .load(source.path)
    )

    src_cols = [c for c in stream.columns if not c.startswith("_")]
    sys_cols = [c for c in stream.columns if c.startswith("_")]

    columns = (
        [
            "if(_corrupt_record is null, true, false) as _is_valid",
            f"cast(null as timestamp) as {source.slice_date_column_name}",
            f"cast({process_id} as long) as _process_id",
            "current_timestamp() as _load_date",
            "_metadata",
        ]
        + src_cols
        + sys_cols

    )

    stream = stream.selectExpr(*columns)
    stream = source.add_timeslice(stream)
    destination.create_table(schema=stream.schema)

    if drop_already_loaded:
      stream = drop_if_already_loaded(stream, source)

    stream_data: StreamingQuery = (stream
        .select("*")
        .writeStream
        .options(**destination.options)
        .trigger(availableNow=True)
        .toTable(f"`{catalog.database}`.`{destination.database}`.`{destination.table}`")
    )

    stream_data.awaitTermination()
    if destination.z_order_by:
      z_order_by(destination)


def batch_load(
    process_id: int,
    source: Read,
    destination: DeltaLake,
    drop_already_loaded:bool = True
):
    df:DataFrame = (
        spark.read.schema(source.spark_schema)
        .format(source.format)
        .options(**source.options)
        .load(source.path)
    )

    src_cols = [c for c in df.columns if not c.startswith("_")]
    sys_cols = [c for c in df.columns if c.startswith("_")]

    columns = (
        [
            "if(_corrupt_record is null, true, false) as _is_valid",
            f"cast(null as timestamp) as {source.slice_date_column_name}",
            f"cast({process_id} as long) as _process_id",
            "current_timestamp() as _load_date",
            "_metadata",
        ]
        + src_cols
        + sys_cols

    )

    df = df.selectExpr(*columns)
    df = source.add_timeslice(df)
    destination.create_table(schema=df.schema)

    if drop_already_loaded:
      df = drop_if_already_loaded(df, source)

    audit:DataFrame = (df
        .select("*")
        .write
        .options(**destination.options)
        .mode("append")
        .saveAsTable(name=f"`{catalog.database}`.`{destination.database}`.`{destination.table}`")
    )
    if destination.z_order_by:
      z_order_by(destination)

    return audit
