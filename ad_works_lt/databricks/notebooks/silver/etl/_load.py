from yetl import Read, DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame
import logging

def z_order_by(
    destination: DeltaLake
):
    _logger = logging.getLogger(__name__)
    _z_order_by = destination.z_order_by
    if isinstance(_z_order_by, list):
      _z_order_by = ",".join(destination.z_order_by)
    print("Optimizing")
    sql = f"""
        OPTIMIZE `{destination.database}`.`{destination.table}`
        ZORDER BY ({_z_order_by})
    """
    _logger.info(sql)
    spark.sql(sql)


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
