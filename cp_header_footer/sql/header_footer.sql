
CREATE TABLE IF NOT EXISTS `{{catalog}}`.`control_header_footer`.`header_footer`
(
    header struct<flag:string,row_count:bigint,period:bigint,batch:string>,
    raw_header string,
    footer struct<flag:string,name:string,period:bigint>,
    raw_footer string,
    file_path string,
    file_name string,
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
);
