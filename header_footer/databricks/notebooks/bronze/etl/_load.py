from yetl import Read, DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql.streaming import StreamingQuery
import hashlib
from pyspark.sql import DataFrame
from typing import Union
from pyspark.sql import functions as fn


def hash_value(value: str):
    hash_object = hashlib.sha224(f"{value}".encode("utf-8"))
    hex_dig = hash_object.hexdigest()
    return hex_dig

def z_order_by(
    destination: DeltaLake
):
    z_order_by = destination.z_order_by
    if isinstance(z_order_by, list): 
      z_order_by = ",".join(z_order_by)
      
    print("Optimizing")
    sql = f"""
        OPTIMIZE `{destination.database}`.`{destination.table}`
        ZORDER BY ({z_order_by})
    """
    print(sql)
    spark.sql(sql)


def drop_if_already_loaded(df:Union[DataFrame, StreamingQuery], source:Read):
    already_loaded = spark.sql(f"""
      select struct(file_path, file_name, file_size, file_modification_time) as _metadata_loaded
      from yetl_control_header_footer.raw_audit
      where source_table = '{source.table}'                   
    """)
    match_on_metadata = [
       "_metadata.file_path",
       "_metadata.file_name",
       "_metadata.file_size",
       "_metadata.file_modification_time"
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
    drop_already_loaded:bool = False
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
        src_cols
        + sys_cols
        + [
            "if(_corrupt_record is null, true, false) as _is_valid",
            f"cast(null as timestamp) as {source.slice_date_column_name}",
            f"cast({process_id} as long) as _process_id",
            "current_timestamp() as _load_date",
            "_metadata",
        ]
    )

    stream = stream.selectExpr(*columns)
    stream = source.add_timeslice(stream)

    if drop_already_loaded:
      drop_if_already_loaded(stream, source)

    stream_data: StreamingQuery = (stream
        .select("*")
        .writeStream
        .options(**destination.options)
        .trigger(availableNow=True)
        .toTable(f"`{destination.database}`.`{destination.table}`")
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
        src_cols
        + sys_cols
        + [
            "if(_corrupt_record is null, true, false) as _is_valid",
            f"cast(null as timestamp) as {source.slice_date_column_name}",
            f"cast({process_id} as long) as _process_id",
            "current_timestamp() as _load_date",
            "_metadata",
        ]
    )

    df = df.selectExpr(*columns)
    df = source.add_timeslice(df)

    if drop_already_loaded:
      drop_if_already_loaded(df, source)

    audit:DataFrame = (df
        .select("*")
        .write
        .options(**destination.options)
        .mode("append")
        .saveAsTable(name=f"`{destination.database}`.`{destination.table}`")
    )
    if destination.z_order_by:
      z_order_by(destination)

    return audit
