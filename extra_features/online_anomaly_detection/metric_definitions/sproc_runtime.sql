SELECT
  sproc_name as ident,
  sp_timestamp AS dt,
  max(value)::int as value
FROM (
    SELECT
      sproc_name,
      sp_timestamp,
      (total_time - total_time_lag) / (calls - calls_lag)::numeric AS value
    FROM (
      SELECT
        sp_timestamp,
        sp_sproc_id,
        split_part(sproc_name, '(', 1) as sproc_name,
        sp_calls AS calls,
        lag(sp_calls) OVER w AS calls_lag,
        sp_total_time AS total_time,
        lag(sp_total_time) OVER w AS total_time_lag
      FROM
        monitor_data.sproc_performance_data
        JOIN
        monitor_data.sprocs ON sproc_id = sp_sproc_id
      WHERE
        sp_host_id = %(host_id)s
        AND sproc_host_id = %(host_id)s
        AND (%(filter)s is null or sproc_name like any(select unnest(%(filter)s) || '(%%'))
        AND sp_timestamp > %(date_from)s::timestamp - interval '12hours'
        AND (%(date_to)s IS NULL OR sp_timestamp < %(date_to)s)
      WINDOW w AS (PARTITION BY sp_sproc_id ORDER BY sp_timestamp)
    ) a
    WHERE
      total_time - total_time_lag >= 0
      AND calls - calls_lag > 0
      AND sp_timestamp > %(date_from)s::timestamp
) b
GROUP BY
  sproc_name, sp_timestamp
ORDER BY
  sproc_name, sp_timestamp