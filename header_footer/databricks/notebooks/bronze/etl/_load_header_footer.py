from yetl import DeltaLake
from databricks.sdk.runtime import spark
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame


def get_header_schema():
  return ",".join(["flag string", "row_count long", "period long", "batch string"])

def get_footer_schema():
  return ",".join(["flag string", "name string", "period long"])

def get_header_columns():
  return [
    f"from_csv(_corrupt_record, '{get_header_schema()}') as header",
    "_corrupt_record as raw_header",
    "_slice_date",
    "_process_id",
    "_load_date",
    "_metadata.file_path",
    "_metadata.file_name",
    "_metadata.file_size",
    "_metadata.file_modification_time",
    "_metadata.file_block_start",
    "_metadata.file_block_length"
  ]  

def get_footer_columns():
  return [
    f"from_csv(_corrupt_record, '{get_footer_schema()}') as footer",
    "_corrupt_record as raw_footer",
    "_slice_date as f_slice_date",
    "_process_id as f_process_id",
    "_metadata.file_name as f_file_name",
  ]

def get_columns():
  return [
      "header",
      "raw_header",
      "footer",
      "raw_footer",
      "file_path",
      "file_name",
      "file_size",
      "file_modification_time",
      "file_block_start",
      "file_block_length",
      "_slice_date",
      "_process_id",
      "_load_date"
  ]

def stream_load_header_footer(
    process_id: int,
    source: DeltaLake, 
    destination: DeltaLake
):

    columns = get_header_columns()
    # https://docs.databricks.com/delta/delta-change-data-feed.html
    stream_header: StreamingQuery = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(source.qualified_table_name())
        .where("_change_type = 'insert' and flag = 'H'")
        .selectExpr(*columns)
    )

    columns = get_footer_columns()

    stream_footer: StreamingQuery = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(source.qualified_table_name())
        .where("_change_type = 'insert' and flag = 'F'")
        .selectExpr(*columns)
    )

    columns = get_columns()

    stream_joined = stream_header.join(
        stream_footer,
        fn.expr(
            """
        _process_id = f_process_id AND
        file_name = f_file_name AND
        _slice_date = f_slice_date
      """
        ),
    ).selectExpr(*columns)

    stream_write = (
        stream_joined.writeStream.options(**destination.options)
        .trigger(availableNow=True)
        .toTable(destination.qualified_table_name())
    )

    stream_write.awaitTermination()


def batch_load_header_footer(
    process_id: int,
    source: DeltaLake, 
    destination: DeltaLake
):
  
    columns = get_header_columns()
    # https://docs.databricks.com/delta/delta-change-data-feed.html
    df_header: DataFrame = (
        spark.read.format("delta")
        .table(source.qualified_table_name())
        .where(f"_process_id = {process_id} and flag = 'H'")
        .selectExpr(*columns)
    )

    columns = get_footer_columns()
    df_footer: DataFrame = (
        spark.read.format("delta")
        .table(source.qualified_table_name())
        .where(f"_process_id = {process_id} and flag = 'F'")
        .selectExpr(*columns)
    )

    columns = get_columns()

    df_joined = df_header.join(
        df_footer,
        fn.expr("""
        _process_id = f_process_id AND
        file_name = f_file_name AND
        _slice_date = f_slice_date
        """),
    ).selectExpr(*columns)

    df_write = df_joined.write
    
    if destination.options:
      df_write = df_write.options(**destination.options)

    df_write = (df_write
      .mode("append")
      .saveAsTable(destination.qualified_table_name())
    )

