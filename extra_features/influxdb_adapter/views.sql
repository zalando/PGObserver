/*

these views are adapters to feed InfluxDB. all columns except "timestamp" and "host_id" will be pushed into a series
 named by a mapping dict VIEW_TO_SERIES_MAPPING in influxdb_importer.py

*/

create or replace view monitor_data.v_influx_load
as
  select
    load_host_id as host_id,
    load_timestamp as "timestamp",
    extract(epoch from load_timestamp) as time,
    round(load_1min_value::numeric/100.0, 1)::float as "1min",
    round(load_5min_value::numeric/100.0, 1)::float as "5min",
    round(load_15min_value::numeric/100.0, 1)::float as "15min",
    xlog_location_mb * 10^6 as xlog_b
  from
    monitor_data.host_load
  where
    load_timestamp <= now() - '1minute'::interval;    -- this is a "safety" to not to show fully inserted datasets

grant select on monitor_data.v_influx_load to pgobserver_frontend;


create or replace view monitor_data.v_influx_db_info
as
  select
    sdd_host_id as host_id,
    sdd_timestamp as "timestamp",
    extract(epoch from sdd_timestamp) as time,
    sdd_numbackends as conns,
    sdd_xact_commit as commits,
    sdd_xact_rollback as rollbacks,
    sdd_blks_read as blks_read,
    sdd_blks_hit as blks_hit,
    sdd_temp_files as temp_files,
    sdd_temp_bytes as temp_bytes,
    sdd_deadlocks as deadlocks,
    sdd_blk_read_time as blk_read_time_ms,
    sdd_blk_write_time as blk_write_time_ms
  from
    monitor_data.stat_database_data
  where
    sdd_timestamp <= now() - '1minute'::interval;

grant select on monitor_data.v_influx_db_info to pgobserver_frontend;


create or replace view monitor_data.v_influx_sproc_info
as
  select
    sp_host_id as host_id,
    sp_timestamp as "timestamp",
    extract(epoch from sp_timestamp) as time,
    sproc_schema||'.'||sproc_name as sproc,
    sp_calls as calls,
    sp_total_time as total_ms,
    sp_self_time as self_ms
  from
    monitor_data.sproc_performance_data
    join
    monitor_data.sprocs on sproc_id = sp_sproc_id
  where
    sp_timestamp <= now() - '1minute'::interval;

grant select on monitor_data.v_influx_sproc_info to pgobserver_frontend;


/*

should get the lag values for every timepoint? in grafana we lose precision

create or replace view monitor_data.v_influx_sproc_info2
as
  select
    sp_host_id as host_id,
    sp_timestamp as "timestamp",
    extract(epoch from sp_timestamp) as time,
    sproc_schema||'.'||sproc_name as sproc,
    sp_calls as calls,
    sp_total_time as total_ms,
    sp_self_time as self_ms,
    COALESCE(sp_calls - lag(sp_calls) OVER w, 0::bigint) AS delta_calls,
    COALESCE(sp_total_time - lag(sp_total_time) OVER w, 0::bigint) AS delta_total
  from
    monitor_data.sproc_performance_data
    join
    monitor_data.sprocs on sproc_id = sp_sproc_id
  where
    sp_timestamp <= now() - '1minute'::interval
  window w as
    ( PARTITION BY sp_sproc_id ORDER BY sp_timestamp );

grant select on monitor_data.v_influx_sproc_info2 to pgobserver_frontend;
*/

create or replace view monitor_data.v_influx_table_info
as
  select
    tsd_host_id as host_id,
    tsd_timestamp as timestamp,
    extract(epoch from tsd_timestamp) as time,
    t_schema||'.'||t_name as name,
    tsd_table_size as tsize_b,  /* MB? */
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
    not t_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', '\_v'])
    and tsd_timestamp <= now() - '1minute'::interval;

grant select on monitor_data.v_influx_table_info to pgobserver_frontend;



-- TODO lag
create or replace view monitor_data.v_influx_table_io_info
as
  select
    tio_host_id as host_id,
    tio_timestamp as timestamp,
    extract(epoch from tio_timestamp) as time,
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
    not t_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', '\_v'])
    and tio_timestamp <= now() - '1minute'::interval;


grant select on monitor_data.v_influx_table_io_info to pgobserver_frontend;


create or replace view monitor_data.v_influx_blocked_processes
as
  select
    bp_host_id as host_id,
    bp_timestamp as timestamp,
    extract(epoch from bp_timestamp) as time,
    count(1)
  from
    monitor_data.blocking_processes
  where
    waiting
    and bp_timestamp <= now() - '1minute'::interval
  group by
    1, 2, 3
  order by
    1, 2;


grant select on monitor_data.v_influx_blocked_processes to pgobserver_frontend;


create or replace view monitor_data.v_influx_index_info
as
  select
    iud_host_id as host_id,
    iud_timestamp as timestamp,
    extract(epoch from iud_timestamp) as time,
    i_schema||'.'|| i_name as name,
    i_schema||'.'|| i_table_name as table_name,
    iud_scan as scans,
    iud_size as size,
    iud_tup_read as tup_read,
    iud_tup_fetch as tup_fetch
  from
    monitor_data.index_usage_data
    join
    monitor_data.indexes on i_id = iud_index_id
  where
    not i_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', '\_v'])
    and iud_timestamp <= now() - '1minute'::interval;


grant select on monitor_data.v_influx_table_info to pgobserver_frontend;


/*
SELECT TIMESTAMP WITH TIME ZONE 'epoch' + 1422379452000 * '1ms'::interval;
*/