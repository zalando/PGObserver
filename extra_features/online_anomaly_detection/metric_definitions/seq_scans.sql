select
  table_name as ident,
  tsd_timestamp as dt,
  ((tsd_seq_scans - lag_scans) / extract(epoch from (tsd_timestamp - lag_timestamp))::numeric * 3600)::int8 as value
from (
    select
      t_id,
      tsd_timestamp,
      tsd_seq_scans,
      lag(tsd_seq_scans) over (partition by t_id order by tsd_timestamp) as lag_scans,
      lag(tsd_timestamp) over (partition by t_id order by tsd_timestamp) as lag_timestamp,
      t_schema||'.'||t_name as table_name
    from
      monitor_data.tables
      join
      monitor_data.table_size_data on tsd_table_id = t_id
    where
      t_host_id = %(host_id)s
      and tsd_host_id = %(host_id)s
      and (%(filter)s is null or t_schema||'.'||t_name = any(%(filter)s))
      and tsd_timestamp >= (%(date_from)s::timestamp - interval '12hours')
      and (%(date_to)s IS NULL OR tsd_timestamp < %(date_to)s::timestamp)
      and tsd_timestamp < now() - '60 seconds'::interval -- safety interval as data is not inserted in one tx by the gatherer
) a
where
  tsd_seq_scans - lag_scans >= 0
  and tsd_timestamp > lag_timestamp
  and tsd_timestamp > %(date_from)s
order by
  table_name, tsd_timestamp