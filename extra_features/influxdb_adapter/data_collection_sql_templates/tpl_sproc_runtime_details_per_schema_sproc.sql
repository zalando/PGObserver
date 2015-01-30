select
  %(host_id)s as host_id,
  sp_timestamp as "timestamp",
  extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc') as "time",
  sproc_schema as "schema",
  sproc,
  (calls_delta / ((extract (epoch from timestamp_delta)) / 3600.0))::int8 as calls_per_hour_rate,
  (total_delta * 1000 / 3600.0)::int8 as duration_per_hour_rate,
  (self_delta / calls_delta * 1000)::int8 as avg_self_us,
  (total_delta / calls_delta * 1000)::int8 as avg_total_us
from
  (
    select
      sp_timestamp,
      sproc_schema,
      sproc,
      sp_timestamp - timestamp_lag as timestamp_delta,
      calls - calls_lag as calls_delta,
      total_ms - total_ms_lag as total_delta,
      self_ms - self_ms_lag as self_delta
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
        sp_timestamp <= now() - '1minute'::interval
        and case
              when %(last_timestamp)s is null then
                sp_timestamp > current_date - %(max_days)s
              else
                sp_timestamp > coalesce(%(last_timestamp)s, now()) - '1 hour'::interval
            end        
        and sp_host_id = %(host_id)s
      order by
        4,2
      ) a
      where
        calls - calls_lag > 0   -- won't show data for unchanged sprocs and "stats resets"
        and total_ms - total_ms_lag > 0
        and self_ms - self_ms_lag > 0
) b
where
  sp_timestamp > coalesce(%(last_timestamp)s, current_date - %(max_days)s)
order by
 "schema", sproc, sp_timestamp
;
