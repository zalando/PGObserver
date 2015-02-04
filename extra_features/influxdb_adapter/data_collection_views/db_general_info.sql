drop view if exists monitor_data.v_influx_db_info;

create or replace view monitor_data.v_influx_db_info
as
  select
    sdd_host_id as host_id,
    sdd_timestamp as "timestamp",
    extract(epoch from sdd_timestamp::timestamp with time zone at time zone 'utc') as "time",
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
