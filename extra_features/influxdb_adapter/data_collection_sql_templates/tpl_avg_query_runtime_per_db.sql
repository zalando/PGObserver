select
  ssd_host_id as host_id,
  ssd_timestamp as "timestamp",
  extract(epoch from ssd_timestamp::timestamp with time zone at time zone 'utc') as "time",
  round( (sum(total_millis) - sum(lag_total_millis)) / (sum(calls) - sum(lag_calls)) ) as avg_ms
from
 (
    select
      *
    from (
        select
          ssd_host_id,
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
          and case
                when %(last_timestamp)s is null then
                  ssd_timestamp > current_date - %(max_days)s
                else
                  ssd_timestamp > coalesce(%(last_timestamp)s, now()) - '1 hour'::interval
              end
          and ssd_calls > 100   -- mininum amount of calls for the sproc to be considered as a "regular"
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
  ssd_timestamp > coalesce(%(last_timestamp)s, current_date - %(max_days)s)
  and ssd_timestamp < now() - '1minute'::interval;
group by
  ssd_host_id, ssd_timestamp
order by
  ssd_host_id, ssd_timestamp
;
