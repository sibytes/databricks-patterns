  CREATE TABLE IF NOT EXISTS `{{catalog}}`.`yetl_control_header_footer_uc`.`base_audit`
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
    expected_row_count bigint,
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
    file_block_start bigint,
    file_block_length bigint,
    _slice_date timestamp,
    _process_id bigint,
    _load_date timestamp
  )
  USING DELTA
  TBLPROPERTIES (
    {{delta_properties}}
  )

  