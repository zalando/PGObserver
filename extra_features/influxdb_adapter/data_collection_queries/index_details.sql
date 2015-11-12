select
  iud_timestamp as "timestamp",
  extract(epoch from iud_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  i_schema as "schema",
  i_name as "index",
  ((scans_delta / timestamp_delta_s) * 3600) ::int8 as scans_1h_rate,
  size_b
from
(
select
  *,
  extract (epoch from (iud_timestamp - timestamp_lag))::numeric as timestamp_delta_s,
  scans - scans_lag as scans_delta
from
  (
select
  *,
  lag(iud_timestamp) over(partition by i_schema, i_name order by iud_timestamp) as timestamp_lag,
  lag(scans) over(partition by i_schema, i_name order by iud_timestamp) as scans_lag
from (
  select
    iud_timestamp,
    i_schema,
    i_name,
    max(iud_scan) as scans,
    max(iud_size) as size_b
    --iud_tup_read as tup_read,
    --iud_tup_fetch as tup_fetch
  from
    monitor_data.index_usage_data
    join
    monitor_data.indexes on i_id = iud_index_id
  where
    not i_schema like any(array['pg_temp%%', 'z_blocking', 'tmp%%', 'temp%%', E'\\_v'])
    and iud_timestamp > %(from_timestamp)s - %(lag_interval)s::interval
    and iud_timestamp <= %(to_timestamp)s
    and iud_host_id = %(host_id)s
  group by
    1, 2, 3
) a
where
  scans > 10
) b
where
  scans - scans_lag >= 0
  and size_b >= 10 * 10^6    -- 10 MB min
  and iud_timestamp > %(from_timestamp)s
) c
where
  timestamp_delta_s > 1
order by
  i_schema, i_name, iud_timestamp
