select
  sp_timestamp as "timestamp",
  extract(epoch from sp_timestamp::timestamp with time zone at time zone 'utc') as "time",
  ((calls_delta / extract (epoch from timestamp_delta)) * 60)::int8 as calls_1m_rate,
  (self_delta / calls_delta * 1000)::int8 as avg_self_us,
  (total_delta / calls_delta * 1000)::int8 as avg_total_us
from
  (

select
  sp_timestamp,
  sp_timestamp - timestamp_lag as timestamp_delta,
  calls - calls_lag as calls_delta,
  self_ms - self_ms_lag as self_delta,
  total_ms - total_ms_lag as total_delta
from
  (
    select
      *,
      lag(sp_timestamp) over(order by sp_timestamp) AS timestamp_lag,
      lag(calls) over(order by sp_timestamp) AS calls_lag,
      lag(self_ms) over(order by sp_timestamp) AS self_ms_lag,
      lag(total_ms) over(order by sp_timestamp) AS total_ms_lag
    from
      (
      select
        sp_timestamp,
        sum(sp_calls) as calls,
        sum(sp_self_time) as self_ms,
        sum(sp_total_time) as total_ms
      from
        monitor_data.sproc_performance_data
        join
        monitor_data.sprocs on sproc_id = sp_sproc_id
      where
        sp_calls > 10
        and sp_timestamp > %(from_timestamp)s - '1 hour'::interval
        and sp_timestamp <= %(to_timestamp)s
        and sp_host_id = %(host_id)s
      group by
        sp_timestamp
      order by
        sp_timestamp
      ) a
) b
where
  self_ms - self_ms_lag >= 0
  and total_ms - total_ms_lag >= 0
  and calls - calls_lag > 0     -- won't show data for unchanged sprocs or in case of stats reset
) c
where
  sp_timestamp > %(from_timestamp)s
order by
  sp_timestamp
