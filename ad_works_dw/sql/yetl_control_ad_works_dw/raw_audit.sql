  CREATE TABLE IF NOT EXISTS `yetl_control_ad_works_dw`.`raw_audit`
  (

    `file_name` string,
    source_database string,
    source_table string,
    `database` string,
    `table` string,
    total_count bigint,
    valid_count bigint,
    invalid_count bigint,
    invalid_ratio double,
    warning_thresholds struct<
      invalid_ratio:double,
      invalid_rows:bigint,
      max_rows:bigint,
      min_rows:bigint
    >,
    exception_thresholds struct<
      invalid_ratio:double,
      invalid_rows:bigint,
      max_rows:bigint,
      min_rows:bigint
    >,
    file_path string,
    file_size bigint,
    file_modification_time timestamp,
    _slice_date timestamp,
    _process_id bigint,
    _load_date timestamp
  )
  USING DELTA
  LOCATION '{{location}}'
  TBLPROPERTIES (
    {{delta_properties}}
  )

  