drop view if exists monitor_data.v_influx_table_info;

-- TODO fan out
create or replace view monitor_data.v_influx_table_info
as
  select
    tsd_host_id as host_id,
    tsd_timestamp as timestamp,
    extract(epoch from tsd_timestamp::timestamp with time zone at time zone 'utc') as time,
    t_schema||'.'||t_name as name,
    tsd_table_size as tsize_b,
    tsd_index_size as isize_b,
    tsd_seq_scans as scans,
    tsd_tup_ins as ins,
    tsd_tup_upd as upd,
    tsd_tup_del as del
  from
    monitor_data.table_size_data
    join
    monitor_data.tables on t_id = tsd_table_id
  where
    not t_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', E'\\_v']);

grant select on monitor_data.v_influx_table_info to pgobserver_frontend;
