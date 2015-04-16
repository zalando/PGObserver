select
    tsd_timestamp as "timestamp",
    extract(epoch from tsd_timestamp::timestamp with time zone at time zone 'utc') as "time",
    t_schema as "schema",
    t_name as "table",
    tsize_b,
    isize_b,
    ((tscans_delta / timestamp_delta_s) * 3600) ::int8 as tscans_1h_rate,
    ((iscans_delta / timestamp_delta_s) * 3600) ::int8 as iscans_1h_rate,
    ((ins_delta / timestamp_delta_s) * 3600) ::int8 as ins_1h_rate,
    ((upd_delta / timestamp_delta_s) * 3600) ::int8 as upd_1h_rate,
    ((del_delta / timestamp_delta_s) * 3600) ::int8 as del_1h_rate
from
(

select
    t_schema,
    t_name,
    tsd_timestamp,
    extract (epoch from (tsd_timestamp - timestamp_lag)) as timestamp_delta_s,
    tsize as tsize_b,
    isize as isize_b,
    tscans - tscans_lag as tscans_delta,
    iscans - iscans_lag as iscans_delta,
    ins - ins_lag as ins_delta,
    upd - upd_lag as upd_delta,
    del - del_lag as del_delta
from
  (

select
  *,
  lag(tsd_timestamp) over(partition by t_id order by tsd_timestamp) as timestamp_lag,
  lag(tscans) over(partition by t_id order by tsd_timestamp) as tscans_lag,
  lag(iscans) over(partition by t_id order by tsd_timestamp) as iscans_lag,
  lag(ins) over(partition by t_id order by tsd_timestamp) as ins_lag,
  lag(upd) over(partition by t_id order by tsd_timestamp) as upd_lag,
  lag(del) over(partition by t_id order by tsd_timestamp) as del_lag
from (
  select
    tsd_timestamp,
    t_id,
    t_schema,
    t_name,
    tsd_table_size as tsize,
    tsd_index_size as isize,
    tsd_seq_scans as tscans,
    tsd_index_scans as iscans,
    tsd_tup_ins as ins,
    tsd_tup_upd as upd,
    tsd_tup_del as del
  from
    monitor_data.table_size_data
    join
    monitor_data.tables on t_id = tsd_table_id
  where
    not t_schema like any(array['pg_temp%%', 'z_blocking', 'tmp%%', 'temp%%', E'\\_v'])
    and tsd_timestamp > %(from_timestamp)s - '1 hour'::interval
    and tsd_timestamp <= %(to_timestamp)s
    and tsd_host_id = %(host_id)s
    and tsd_index_size > 0
    and tsd_table_size > 10 * 10^6  -- min. size 10 MB

) a

) b

) c
where
  tscans_delta >= 0     -- stats reset
  and tsd_timestamp > %(from_timestamp)s
  and timestamp_delta_s > 0
order by
  t_schema, t_name, tsd_timestamp
