from yetl import DeltaLake, ValidationThresholdType
from databricks.sdk.runtime import spark

def load_audit(
  process_id:int,
  source:DeltaLake,
  source_hf:DeltaLake,
  destination:DeltaLake
):
  exception_thresholds_sql = destination.thresholds_select_sql(ValidationThresholdType.exception)
  warning_thresholds_sql = destination.thresholds_select_sql(ValidationThresholdType.warning)

  df = spark.sql(f"""
    SELECT
      '{source.database}' as `database`,
      '{source.table}' as `table`,
      cast(count(*) as long) as total_count,
      cast(sum(if(d._is_valid, 1, 0)) as long) as valid_count,
      cast(sum(if(d._is_valid, 0, 1)) as long) as invalid_count,
      if(ifnull(cast(count(*) as long), 0)=0, 0.0,
        cast(sum(if(d._is_valid, 0, 1)) as long) / cast(count(*) as long)
      ) as invalid_ratio,
      hf.header.row_count as expected_row_count,
      {warning_thresholds_sql} as warning_thresholds,
      {exception_thresholds_sql} as exception_thresholds,
      hf._process_id,
      hf._load_date,
      d._metadata.file_name,
      d._metadata.file_path,
      d._metadata.file_size,
      d._metadata.file_modification_time
    FROM `{source.database}`.`{source.table}` as d
    JOIN `{source_hf.database}`.`{source_hf.table}` as hf
      ON hf._process_id = d._process_id
      AND hf._metadata.file_name = d._metadata.file_name
    WHERE d._process_id = {process_id}
    GROUP BY 
      hf.header.row_count,
      hf._process_id,
      hf._load_date,
      d._metadata.file_name,
      d._metadata.file_path,
      d._metadata.file_size,
      d._metadata.file_modification_time
  """)

  columns = [
    "total_count",
    "valid_count",
    "invalid_count",
    "invalid_ratio",
    "expected_row_count",
    "_process_id",
    "_load_date",
    "file_name",
    "file_path",
    "file_size",
    "file_modification_time"
  ]
  result = (df.select(*columns).write
    .format("delta")
    .mode("append")
    .saveAsTable(f"`{destination.database}`.`{destination.table}`")
  )
  return result

