-- TODO should be moving avg. based
drop view if exists monitor_data.v_influx_sproc_info;

create or replace view monitor_data.v_influx_sproc_info
as
  select
    sp_host_id as host_id,
    sp_timestamp as "timestamp",
    extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc') as time,
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
    extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc') as time,
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
