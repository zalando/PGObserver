select
  bp_timestamp as "timestamp",
  extract(epoch from bp_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  count(1)
from
  monitor_data.blocking_processes
where
  waiting
  and bp_timestamp > %(from_timestamp)s
  and bp_timestamp <= %(to_timestamp)s
  and bp_host_id = %(host_id)s
group by
  bp_timestamp
order by
  bp_timestamp;

