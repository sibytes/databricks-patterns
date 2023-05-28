from yetl import Read, DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql.streaming import StreamingQuery
import hashlib
from pyspark.sql import DataFrame


def hash_value(value: str):
    hash_object = hashlib.sha224(f"{value}".encode("utf-8"))
    hex_dig = hash_object.hexdigest()
    return hex_dig

def z_order_by(
    process_id: int,
    destination: DeltaLake
):

    z_order_by = ",".join(destination.z_order_by)
    print("Optimizing")
    sql = f"""
        OPTIMIZE `{destination.database}`.`{destination.table}`
        ZORDER BY ({z_order_by})
    """
    print(sql)
    spark.sql(sql)



def stream_load(
    process_id: int,
    source: Read,
    destination: DeltaLake
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
      z_order_by(process_id, destination)


def batch_load(
    process_id: int,
    source: Read,
    destination: DeltaLake
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
      z_order_by(process_id, destination)

    return audit