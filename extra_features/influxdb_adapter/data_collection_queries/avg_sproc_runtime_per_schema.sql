select
  sp_timestamp as "timestamp",
  extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  sproc_schema as "schema",
  ((calls_delta / extract (epoch from timestamp_delta)) * 3600) ::int8 as calls_1h_rate,
  (self_delta / calls_delta * 1000)::int8 as avg_self_us,
  (total_delta / calls_delta * 1000)::int8 as avg_total_us
from
  (

select
  sproc_schema,
  sp_timestamp,
  sp_timestamp - timestamp_lag as timestamp_delta,
  calls - calls_lag as calls_delta,
  self_ms - self_ms_lag as self_delta,
  total_ms - total_ms_lag as total_delta
from
  (
    select
      *,
      lag(sp_timestamp) over(partition by sproc_schema order by sp_timestamp) AS timestamp_lag,
      lag(calls) over(partition by sproc_schema order by sp_timestamp) AS calls_lag,
      lag(self_ms) over(partition by sproc_schema order by sp_timestamp) AS self_ms_lag,
      lag(total_ms) over(partition by sproc_schema order by sp_timestamp) AS total_ms_lag
    from
      (
      select
        sproc_schema,
        sp_timestamp,
        sum(sp_calls) as calls,
        sum(sp_self_time) as self_ms,
        sum(sp_total_time) as total_ms
      from
        monitor_data.sproc_performance_data
        join
        monitor_data.sprocs on sproc_id = sp_sproc_id
      where
        sp_timestamp > %(from_timestamp)s - '1 hour'::interval
        and sp_timestamp <= %(to_timestamp)s
        and sp_host_id = %(host_id)s
        and sp_calls > 10
      group by
        1, 2
      order by
        1, 2

      ) a
) b
where
  calls - calls_lag > 0   -- won't show data for unchanged sprocs or in case of stats reset
  and self_ms - self_ms_lag >= 0
  and total_ms - total_ms_lag >= 0
) c
where
  sp_timestamp > %(from_timestamp)s
order by
  sproc_schema, sp_timestamp
