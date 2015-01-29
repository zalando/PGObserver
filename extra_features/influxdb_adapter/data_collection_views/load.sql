drop view if exists monitor_data.v_influx_load;

create or replace view monitor_data.v_influx_load
as
  select
    load_host_id as host_id,
    load_timestamp as "timestamp",
    extract(epoch from load_timestamp::timestamp with time zone at time zone 'utc') as "time",
    round(load_1min_value::numeric/100.0, 1)::float as "1min",
    round(load_5min_value::numeric/100.0, 1)::float as "5min",
    round(load_15min_value::numeric/100.0, 1)::float as "15min",
    xlog_location_mb * 10^6 as xlog_b
  from
    monitor_data.host_load
  where
    load_timestamp <= now() - '1minute'::interval;    -- this is a "safety" to not to show fully inserted datasets

grant select on monitor_data.v_influx_load to pgobserver_frontend;


/*
SELECT TIMESTAMP WITH TIME ZONE 'epoch' + 1422379452000 * '1ms'::interval;
*/
