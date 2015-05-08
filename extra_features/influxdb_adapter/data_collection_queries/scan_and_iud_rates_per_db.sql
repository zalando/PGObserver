select
    tsd_timestamp as "timestamp",
    extract(epoch from tsd_timestamp::timestamp with time zone at time zone 'utc') as "time",
    ((tscans_delta / timestamp_delta_s) * 3600) ::int8 as tscans_1h_rate,
    ((iscans_delta / timestamp_delta_s) * 3600) ::int8 as iscans_1h_rate,
    ((ins_delta / timestamp_delta_s) * 3600) ::int8 as ins_1h_rate,
    ((upd_delta / timestamp_delta_s) * 3600) ::int8 as upd_1h_rate,
    ((del_delta / timestamp_delta_s) * 3600) ::int8 as del_1h_rate
from
(

select
    tsd_timestamp,
    extract (epoch from (tsd_timestamp - timestamp_lag)) as timestamp_delta_s,
    tscans - tscans_lag as tscans_delta,
    iscans - iscans_lag as iscans_delta,
    ins - ins_lag as ins_delta,
    upd - upd_lag as upd_delta,
    del - del_lag as del_delta
from
  (

select
  *,
  lag(tsd_timestamp) over(order by tsd_timestamp) as timestamp_lag,
  lag(tscans) over(order by tsd_timestamp) as tscans_lag,
  lag(iscans) over(order by tsd_timestamp) as iscans_lag,
  lag(ins) over(order by tsd_timestamp) as ins_lag,
  lag(upd) over(order by tsd_timestamp) as upd_lag,
  lag(del) over(order by tsd_timestamp) as del_lag
from (
  select
    tsd_timestamp,
    sum(tsd_seq_scans) as tscans,
    sum(tsd_index_scans) as iscans,
    sum(tsd_tup_ins) as ins,
    sum(tsd_tup_upd) as upd,
    sum(tsd_tup_del) as del
  from
    monitor_data.table_size_data
    join
    monitor_data.tables on t_id = tsd_table_id
  where
    tsd_timestamp > %(from_timestamp)s - '1 hour'::interval
    and tsd_timestamp <= %(to_timestamp)s
    and tsd_host_id = %(host_id)s
  group by
    tsd_timestamp
) a

) b

) c
where
  tscans_delta >= 0
  and iscans_delta >= 0
  and not (tscans_delta = 0 and iscans_delta = 0 and ins_delta = 0 and upd_delta = 0 and del_delta = 0)
  and tsd_timestamp > %(from_timestamp)s
  and timestamp_delta_s > 0
order by
  tsd_timestamp
