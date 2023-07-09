from yetl import DeltaLake, ValidationThresholdType
from databricks.sdk.runtime import spark
import logging


def load_audit(
    process_id: int,
    landing: DeltaLake,
    raw: DeltaLake,
    destination: DeltaLake
):
    _logger = logging.getLogger(__name__)

    exception_thresholds_sql = raw.thresholds_select_sql(
        ValidationThresholdType.exception
    )
    warning_thresholds_sql = raw.thresholds_select_sql(
        ValidationThresholdType.warning
    )

    sql =f"""
      SELECT
        d._metadata.file_name,
        '{landing.database}' as `source_database`,
        '{landing.table}'    as `source_table`,
        '{raw.database}'     as `database`,
        '{raw.table}'        as `table`,

        cast(count(*) as long)                   as total_count,
        cast(sum(if(d._is_valid, 1, 0)) as long) as valid_count,
        cast(sum(if(d._is_valid, 0, 1)) as long) as invalid_count,
        if(ifnull(cast(count(*) as long), 0)=0, 0.0,
          cast(sum(if(d._is_valid, 0, 1)) as long) / cast(count(*) as long)
        )                                        as invalid_ratio,
        {warning_thresholds_sql}                 as warning_thresholds,
        {exception_thresholds_sql}               as exception_thresholds,
        d._metadata.file_path,
        d._metadata.file_size,
        d._metadata.file_modification_time,
        d._metadata.file_block_start,
        d._metadata.file_block_length,
        d._slice_date,
        d._process_id,
        d._load_date
      FROM `{raw.database}`.`{raw.table}` as d
      WHERE d._process_id = {process_id}
      GROUP BY
        warning_thresholds,
        exception_thresholds,
        d._metadata.file_name,
        d._metadata.file_path,
        d._metadata.file_size,
        d._metadata.file_modification_time,
        d._metadata.file_block_start,
        d._metadata.file_block_length,
        d._slice_date,
        d._process_id,
        d._load_date
    """


    _logger.debug(sql)
    _logger.info(f"loading table `{destination.database}`.`{destination.table}`")

    df = spark.sql(sql)

    result = (
        df.write.format("delta")
        .mode("append")
        .saveAsTable(f"`{destination.database}`.`{destination.table}`")
    )
    return result
