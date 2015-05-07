select
  ssd_timestamp as "timestamp",
  extract(epoch from ssd_timestamp::timestamp with time zone at time zone 'utc') as "time",
  ((sum(total_millis) - sum(lag_total_millis)) / (sum(calls) - sum(lag_calls)) * 1000)::int8 as avg_us
from
 (
    select
      *
    from (
        select
          ssd_timestamp,
          ssd_query,
          ssd_calls as calls,
          ssd_total_time as total_millis,
          lag(ssd_calls) over(partition by ssd_query order by ssd_timestamp) as lag_calls,
          lag(ssd_total_time) over(partition by ssd_query order by ssd_timestamp) as lag_total_millis
        from
          stat_statements_data
        where
          ssd_host_id = %(host_id)s
          and ssd_timestamp > %(from_timestamp)s - '1hour'::interval    -- 1h should be enough to get a "lag" value
          and ssd_timestamp <= %(to_timestamp)s
          and ssd_calls > 10   -- mininum amount of calls for the statement to be considered as a "regular"
          and lower(ssd_query) not like 'copy%%'
        order by
          ssd_timestamp
    ) a
    where
      calls > lag_calls
    order by
      ssd_query, ssd_timestamp
  ) b
where
  ssd_timestamp > %(from_timestamp)s
group by
  ssd_timestamp
order by
  ssd_timestamp
;
