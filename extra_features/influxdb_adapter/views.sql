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
    load_1min_value as "1min",
    load_5min_value as "5min",
    load_15min_value as "15min",
    xlog_location_mb as xlog_mb
  from
    monitor_data.host_load;

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
    monitor_data.stat_database_data;

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
    monitor_data.sprocs on sproc_id = sp_sproc_id;

grant select on monitor_data.v_influx_sproc_info to pgobserver_frontend;


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
    not t_schema like any(array['pg_temp%', 'z_blocking', 'tmp%', 'temp%', '\_v']);


grant select on monitor_data.v_influx_table_info to pgobserver_frontend;



/*
SELECT TIMESTAMP WITH TIME ZONE 'epoch' + 1421341189 * '1s'::interval;
*/