  CREATE TABLE IF NOT EXISTS `raw_dbx_patterns_control`.`raw_audit_threshold`
  (
    `database` string,
    `table` string,
    threshold_type string,
    invalid_ratio double,
    invalid_rows bigint,
    max_rows bigint,
    min_rows bigint,
    _process_id bigint,
    _load_date timestamp
  )
  USING DELTA
  LOCATION '{{location}}'
  TBLPROPERTIES (
    {{delta_properties}}
  )

