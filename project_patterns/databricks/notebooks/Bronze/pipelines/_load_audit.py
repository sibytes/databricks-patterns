from yetl import DeltaLake, ValidationThreshold
from databricks.sdk.runtime import spark

def load_audit(
  process_id:int,
  source:DeltaLake,
  source_hf:DeltaLake,
  destination:DeltaLake
):

  df = spark.sql(f"""
    SELECT
      cast(count(*) as long) as total_count,
      cast(sum(if(d._is_valid, 1, 0)) as long) as valid_count,
      cast(sum(if(d._is_valid, 0, 1)) as long) as invalid_count,
      if(ifnull(cast(count(*) as long), 0)=0, 0.0,
        cast(sum(if(d._is_valid, 0, 1)) as long) / cast(count(*) as long)
      ) as invalid_ratio,
      hf.header.row_count as expected_row_count,
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


def create_threhsholds_views(
  param_process_id:int,
  destination: DeltaLake,
  thresholds: ValidationThreshold,
  threshold_type: str
):
  if thresholds:
    
    view = f"{threshold_type}_{destination.table}"

    select = []
    where = []
    if thresholds.invalid_ratio is not None:
      select.append(f"{thresholds.invalid_ratio} as threshold_invalid_ratio,")
      where.append(f"invalid_ratio > {thresholds.invalid_ratio}")

    if thresholds.max_rows is not None:
      select.append(f"{thresholds.max_rows} as threshold_min_count,")
      where.append(f"total_count > {thresholds.max_rows}")

    if thresholds.max_rows is not None:
      select.append(f"{thresholds.max_rows} as threshold_max_count,")
      where.append(f"total_count < {thresholds.min_rows}")

    if thresholds.invalid_rows is not None:
      select.append(f"{thresholds.invalid_rows} as threshold_invalid_count,")
      where.append(f"invalid_count > {thresholds.invalid_rows}")

    select = "\\n".join(select)
    where = "OR\\n".join(where)

    sql = f"""
      CREATE VIEW IF NOT EXISTS `{destination.database}`.`{view}`
      AS 
      select
        invalid_ratio,
        total_count,
        expected_row_count,
        valid_count,
        invalid_count,
        {select}
        file_name,
        _process_id
      from `{destination.database}`.`{destination.table}`
      where _process_id = {param_process_id}
      and (
        {where} OR
        expected_row_count != valid_count
      );
    """
    print(sql)
    spark.sql(sql)
    df = spark.sql(f"select * from `{destination.database}`.`{view}`")

    return df