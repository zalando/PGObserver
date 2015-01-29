drop view if exists monitor_data.v_influx_index_info;

create or replace view monitor_data.v_influx_index_info
as
  select
    iud_host_id as host_id,
    iud_timestamp as "timestamp",
    extract(epoch from iud_timestamp::timestamp with time zone at time zone 'utc') as "time",
    i_schema||'.'|| i_name as name,
    i_schema||'.'|| i_table_name as "table",
    iud_scan as scans,
    iud_size as size,
    iud_tup_read as tup_read,
    iud_tup_fetch as tup_fetch
  from
    monitor_data.index_usage_data
    join
    monitor_data.indexes on i_id = iud_index_id
  where
    not i_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', E'\\_v'])
    and iud_timestamp <= now() - '1minute'::interval;


grant select on monitor_data.v_influx_table_info to pgobserver_frontend;
