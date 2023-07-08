from yetl import DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame


def stream_load_header_footer(
    process_id: int,
    source: DeltaLake, 
    destination: DeltaLake
):
    header_schema = ",".join(
        ["flag string", "row_count long", "period long", "batch string"]
    )

    footer_schema = ",".join(["flag string", "name string", "period long"])

    columns = [
        f"from_csv(_corrupt_record, '{header_schema}') as header",
        "_corrupt_record as raw_header",
        "_slice_date",
        "_process_id",
        "_load_date",
        "_metadata",
    ]
    # https://docs.databricks.com/delta/delta-change-data-feed.html
    stream_header: StreamingQuery = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"`{source.database}`.`{source.table}`")
        .where("_change_type = 'insert' and flag = 'H'")
        .selectExpr(*columns)
    )

    columns = [
        f"from_csv(_corrupt_record, '{footer_schema}') as footer",
        "_corrupt_record as raw_footer",
        "_slice_date as f_slice_date",
        "_process_id as f_process_id",
        "_metadata as f_metadata",
    ]
    stream_footer: StreamingQuery = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"`{source.database}`.`{source.table}`")
        .where("_change_type = 'insert' and flag = 'F'")
        .selectExpr(*columns)
    )

    columns = [
        "header",
        "raw_header",
        "footer",
        "raw_footer",
        "_slice_date",
        "_process_id",
        "_load_date",
        "_metadata",
    ]
    stream_joined = stream_header.join(
        stream_footer,
        fn.expr(
            """
        _process_id = f_process_id AND
        _metadata.file_name = f_metadata.file_name AND
        _slice_date = _slice_date
      """
        ),
    ).selectExpr(*columns)

    stream_write = (
        stream_joined.writeStream.options(**destination.options)
        .trigger(availableNow=True)
        .toTable(f"`{destination.database}`.`{destination.table}`")
    )

    stream_write.awaitTermination()


def batch_load_header_footer(
    process_id: int,
    source: DeltaLake, 
    destination: DeltaLake
):
    header_schema = ",".join(
        ["flag string", "row_count long", "period long", "batch string"]
    )

    footer_schema = ",".join(["flag string", "name string", "period long"])

    columns = [
        f"from_csv(_corrupt_record, '{header_schema}') as header",
        "_corrupt_record as raw_header",
        "_slice_date",
        "_process_id",
        "_load_date",
        "_metadata",
    ]
    # https://docs.databricks.com/delta/delta-change-data-feed.html
    df_header: DataFrame = (
        spark.read.format("delta")
        .table(f"`{source.database}`.`{source.table}`")
        .where(f"_process_id = {process_id} and flag = 'H'")
        .selectExpr(*columns)
    )

    columns = [
        f"from_csv(_corrupt_record, '{footer_schema}') as footer",
        "_corrupt_record as raw_footer",
        "_slice_date as f_slice_date",
        "_process_id as f_process_id",
        "_metadata as f_metadata",
    ]
    df_footer: DataFrame = (
        spark.read.format("delta")
        .table(f"`{source.database}`.`{source.table}`")
        .where(f"_process_id = {process_id} and flag = 'F'")
        .selectExpr(*columns)
    )

    columns = [
        "header",
        "raw_header",
        "footer",
        "raw_footer",
        "_slice_date",
        "_process_id",
        "_load_date",
        "_metadata",
    ]
    df_joined = df_header.join(
        df_footer,
        fn.expr("""
        _process_id = f_process_id AND
        _metadata.file_name = f_metadata.file_name AND
        _slice_date = f_slice_date
        """),
    ).selectExpr(*columns)

    df_write = df_joined.write
    if destination.options:
      df_write = df_write.options(**destination.options)
    df_write = (df_write
      .mode("append")
      .saveAsTable(f"`{destination.database}`.`{destination.table}`")
    )

