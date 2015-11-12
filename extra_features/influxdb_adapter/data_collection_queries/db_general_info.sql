select
  sdd_timestamp as "timestamp",
  extract(epoch from sdd_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
  conns,
  ((commits_delta / timestamp_delta_s) * 3600) ::int8 as commits_1h_rate,
  ((rollbacks_delta / timestamp_delta_s) * 3600) ::int8 as rollbacks_1h_rate,
  ((blks_read_delta / timestamp_delta_s) * 3600) ::int8 as blks_read_1h_rate,
  ((blks_hit_delta / timestamp_delta_s) * 3600) ::int8 as blks_hit_hour_rate,
  case when blks_hit_delta + blks_read_delta <= 0
    then null
    else round((blks_hit_delta / (blks_hit_delta + blks_read_delta)::numeric * 100), 1)::double precision
  end as blks_sh_buf_hit_pct,    -- ratio
  ((temp_files_delta / timestamp_delta_s) * 3600) ::int8 as temp_files_hour_rate,
  ((temp_bytes_delta / timestamp_delta_s) * 3600) ::int8 as temp_bytes_1h_rate,
  ((deadlocks_delta / timestamp_delta_s) * 3600) ::int8 as deadlocks_1h_rate,
  ((blk_read_time_ms_delta / timestamp_delta_s) * 3600) ::int8 as blk_read_time_ms_1h_rate,
  ((blk_write_time_ms_delta / timestamp_delta_s) * 3600) ::int8 as blk_write_time_ms_1h_rate,
  case when blk_write_time_ms_delta + blk_read_time_ms_delta <= 0
    then null
    else round((blk_write_time_ms_delta / (blk_write_time_ms_delta + blk_read_time_ms_delta)::numeric * 100), 1)::double precision
  end as blk_write_pct   -- ratio
from (

select
  sdd_timestamp,
  extract (epoch from (sdd_timestamp - timestamp_lag))::numeric as timestamp_delta_s,
  conns,
  commits - commits_lag as commits_delta,
  rollbacks - rollbacks_lag as rollbacks_delta,
  blks_read - blks_read_lag as blks_read_delta,
  blks_hit - blks_hit_lag as blks_hit_delta,
  temp_files - temp_files_lag as temp_files_delta,
  temp_bytes - temp_bytes_lag as temp_bytes_delta,
  deadlocks - deadlocks_lag as deadlocks_delta,
  blk_read_time_ms - blk_read_time_ms_lag as blk_read_time_ms_delta,
  blk_write_time_ms - blk_write_time_ms_lag as blk_write_time_ms_delta
from (
    select
      *,
      lag(sdd_timestamp) over(order by sdd_timestamp) as timestamp_lag,
      lag(commits) over(order by sdd_timestamp) as commits_lag,
      lag(rollbacks) over(order by sdd_timestamp) as rollbacks_lag,
      lag(blks_read) over(order by sdd_timestamp) as blks_read_lag,
      lag(blks_hit) over(order by sdd_timestamp) as blks_hit_lag,
      lag(temp_files) over(order by sdd_timestamp) as temp_files_lag,
      lag(temp_bytes) over(order by sdd_timestamp) as temp_bytes_lag,
      lag(deadlocks) over(order by sdd_timestamp) as deadlocks_lag,
      lag(blk_read_time_ms) over(order by sdd_timestamp) as blk_read_time_ms_lag,
      lag(blk_write_time_ms) over(order by sdd_timestamp) as blk_write_time_ms_lag
    from (
        select
          sdd_timestamp,
          sdd_numbackends as conns,
          sdd_xact_commit as commits,
          sdd_xact_rollback as rollbacks,
          sdd_blks_read as blks_read,
          sdd_blks_hit as blks_hit,
          sdd_temp_files as temp_files,
          sdd_temp_bytes as temp_bytes,
          sdd_deadlocks as deadlocks,
          sdd_blk_read_time as blk_read_time_ms,
          sdd_blk_write_time as blk_write_time_ms
        from
          monitor_data.stat_database_data
        where
          sdd_timestamp > %(from_timestamp)s - %(lag_interval)s::interval
          and sdd_timestamp <= %(to_timestamp)s
          and sdd_host_id = %(host_id)s
    ) a

) b

) c
where
  commits_delta >= 0
  and sdd_timestamp > %(from_timestamp)s
order by
  sdd_timestamp