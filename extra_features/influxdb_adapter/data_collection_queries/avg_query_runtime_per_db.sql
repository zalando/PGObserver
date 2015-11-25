select
  ssd_timestamp as "timestamp",
  extract(epoch from ssd_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  (avg(avg_ms) * 1000)::int8 as avg_us,
  (sum(calls_1s_rate) * 3600)::int8 as calls_1h_rate
from
 (
  select
    ssd_query,
    ssd_timestamp,
    total_millis_delta::numeric / calls_delta as avg_ms,
    calls_delta::numeric / (extract (epoch from timestamp_delta)) as calls_1s_rate
  from (
    select
      *,
      calls - lag_calls as calls_delta,
      ssd_timestamp - lag_ssd_timestamp as timestamp_delta,
      total_millis - lag_total_millis as total_millis_delta
    from (
        select
          ssd_timestamp,
          ssd_query,
          ssd_calls as calls,
          ssd_total_time as total_millis,
          lag(ssd_calls) over(partition by ssd_query_id order by ssd_timestamp) as lag_calls,
          lag(ssd_total_time) over(partition by ssd_query_id order by ssd_timestamp) as lag_total_millis,
          lag(ssd_timestamp) over(partition by ssd_query_id order by ssd_timestamp) as lag_ssd_timestamp
        from
          stat_statements_data
        where
          ssd_host_id = %(host_id)s
          and ssd_timestamp > %(from_timestamp)s - %(lag_interval)s::interval   -- should be enough to get a "lag" value
                                                                                -- statements called less than 1x per 4h will not add to stats
          and ssd_timestamp <= %(to_timestamp)s
          and ssd_calls > 10   -- minimum amount of calls for the statement to be considered as a "regular"
          and lower(ssd_query) not like 'copy%%'
        order by
          ssd_timestamp
    ) a
    where
      calls > lag_calls
      and total_millis > lag_total_millis
      and ssd_timestamp - lag_ssd_timestamp > '1s'::interval
    --order by
    --  ssd_query, ssd_timestamp
  ) b
) c
where
  ssd_timestamp > %(from_timestamp)s
group by
  ssd_timestamp
order by
  ssd_timestamp
;
