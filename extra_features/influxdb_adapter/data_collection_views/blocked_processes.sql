drop view if exists monitor_data.v_influx_blocked_processes;

create or replace view monitor_data.v_influx_blocked_processes
as
  select
    bp_host_id as host_id,
    bp_timestamp as "timestamp",
    extract(epoch from bp_timestamp::timestamp with time zone at time zone 'utc') as "time",
    count(1)
  from
    monitor_data.blocking_processes
  where
    waiting
  group by
    1, 2, 3
  order by
    1, 2;


grant select on monitor_data.v_influx_blocked_processes to pgobserver_frontend;
