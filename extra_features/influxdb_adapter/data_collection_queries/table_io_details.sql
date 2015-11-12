select
    tio_timestamp as "timestamp",
    extract(epoch from tio_timestamp::timestamp with time zone at time zone 'utc')::int as "time",
    t_schema as "schema",
    t_name as "table",
    ((h_read_delta / timestamp_delta_s) * 3600) ::int8 as heap_read_1h_rate,
    ((h_hit_delta / timestamp_delta_s) * 3600) ::int8 as heap_hit_1h_rate,
    case when h_hit_delta + h_read_delta = 0 then null
      else (h_hit_delta / (h_hit_delta + h_read_delta) * 100.0)::int
    end as heap_hit_pct,
    ((i_read_delta / timestamp_delta_s) * 3600) ::int8 as index_read_1h_rate,
    ((i_hit_delta / timestamp_delta_s) * 3600) ::int8 as index_hit_1h_rate,
    case when i_hit_delta + i_read_delta = 0 then null
      else (i_hit_delta / (i_hit_delta + i_read_delta) * 100.0)::int
    end as index_hit_pct
from
(

select
    t_schema,
    t_name,
    tio_timestamp,
    extract (epoch from (tio_timestamp - timestamp_lag))::numeric as timestamp_delta_s,
    h_read_lag - h_read_lag as h_read_delta,
    h_hit - h_hit_lag as h_hit_delta,
    i_read - i_read_lag as i_read_delta,
    i_hit - i_hit_lag as i_hit_delta
from
  (

    select
      *,
      lag(tio_timestamp) over(partition by t_id order by tio_timestamp) as timestamp_lag,
      lag(h_read) over(partition by t_id order by tio_timestamp) as h_read_lag,
      lag(h_hit) over(partition by t_id order by tio_timestamp) as h_hit_lag,
      lag(i_read) over(partition by t_id order by tio_timestamp) as i_read_lag,
      lag(i_hit) over(partition by t_id order by tio_timestamp) as i_hit_lag
    from (
      select
        tio_timestamp,
        t_id,
        t_schema,
        t_name,
        tio_heap_read as h_read,
        tio_heap_hit as h_hit,
        tio_idx_read as i_read,
        tio_idx_hit as i_hit
      from
        monitor_data.table_io_data
        join
        monitor_data.tables on t_id = tio_table_id
      where
        not t_schema like any(array['pg_temp%%', 'z_blocking', 'tmp%%', 'temp%%', E'\\_v'])
        and tio_timestamp > %(from_timestamp)s - %(lag_interval)s::interval
        and tio_timestamp <= %(to_timestamp)s
        and tio_host_id = %(host_id)s

) a

) b

) c
where
  (h_read_delta > 0 or h_hit_delta > 0 or i_read_delta > 0 or i_hit_delta > 0)
  and tio_timestamp > %(from_timestamp)s
order by
  t_schema, t_name, tio_timestamp
