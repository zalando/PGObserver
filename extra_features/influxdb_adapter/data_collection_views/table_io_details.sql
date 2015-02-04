drop view if exists monitor_data.v_influx_table_io_info;

-- TODO lag + fan out
create or replace view monitor_data.v_influx_table_io_info
as
  select
    tio_host_id as host_id,
    tio_timestamp as timestamp,
    extract(epoch from tio_timestamp::timestamp with time zone at time zone 'utc') as time,
    t_schema||'.'||t_name as name,
    tio_heap_read as h_read,
    tio_heap_hit as h_hit,
    100 as h_hit_p,
    tio_idx_read as i_read,
    tio_idx_hit as i_hit,
    100 as i_hit_p
  from
    monitor_data.table_io_data
    join
    monitor_data.tables on t_id = tio_table_id
  where
    not t_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', E'\\_v']);


grant select on monitor_data.v_influx_table_io_info to pgobserver_frontend;
