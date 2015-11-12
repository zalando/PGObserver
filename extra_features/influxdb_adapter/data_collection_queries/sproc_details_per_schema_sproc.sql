select
  sp_timestamp as "timestamp",
  extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  sproc_schema as "schema",
  sproc,
  (((total_delta * 1000) / timestamp_delta_s) * 3600) ::int8 as duration_s_1h_rate,
  ((self_delta * 1000) / calls_delta)::int8 as avg_self_us,
  ((total_delta * 1000) / calls_delta)::int8 as avg_total_us
from
  (
    select
      sp_timestamp,
      sproc_schema,
      sproc,
      extract (epoch from sp_timestamp - timestamp_lag)::numeric as timestamp_delta_s,
      (calls - calls_lag)::numeric as calls_delta,
      (total_ms - total_ms_lag)::numeric as total_delta,
      (self_ms - self_ms_lag)::numeric as self_delta
    from
      (
      select
        sp_timestamp,
        sproc_schema,
        substring(sproc_name, 1, position ('(' in sproc_name)-1) as sproc,   -- TODO sprocs with same names ?
        sp_calls as calls,
        sp_total_time as total_ms,
        sp_self_time as self_ms,
        lag(sp_timestamp) over(partition by sp_sproc_id order by sp_timestamp) AS timestamp_lag,
        lag(sp_calls) over(partition by sp_sproc_id order by sp_timestamp) AS calls_lag,
        lag(sp_total_time) over(partition by sp_sproc_id order by sp_timestamp) AS total_ms_lag,
        lag(sp_self_time) over(partition by sp_sproc_id order by sp_timestamp) AS self_ms_lag
      from
        monitor_data.sproc_performance_data
        join
        monitor_data.sprocs on sproc_id = sp_sproc_id
      where
        sp_timestamp > %(from_timestamp)s - %(lag_interval)s::interval -- sprocs called less than 1x per lag_interval will not add to stats
        and sp_timestamp <= %(to_timestamp)s
        and sp_host_id = %(host_id)s
        and sp_calls > 10
      order by
        4,2
      ) a
      where
        calls - calls_lag > 0   -- won't show data for unchanged sprocs and "stats resets"
        and total_ms - total_ms_lag > 0
        and self_ms - self_ms_lag > 0
) b
where
  sp_timestamp > %(from_timestamp)s
  and timestamp_delta_s > 0
order by
 "schema", sproc, sp_timestamp
;
