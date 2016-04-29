/*
    only return sprocs that take more than 0.5 pct of total sproc daily runtime
*/

WITH q_sproc_total_runtimes_per_day AS (
    SELECT
      sproc_name,
      sp_timestamp::date,
      SUM(total_time - total_time_lag) as runtime_per_day
    FROM (
      SELECT
        sp_timestamp,
        sp_sproc_id,
        split_part(sproc_name, '(', 1) as sproc_name,
        sp_calls AS calls,
        lag(sp_calls) OVER w AS calls_lag,
        sp_total_time::int8 AS total_time,
        lag(sp_total_time::int8) OVER w AS total_time_lag
      FROM
        monitor_data.sproc_performance_data
        JOIN
        monitor_data.sprocs ON sproc_id = sp_sproc_id
      WHERE
        sp_host_id = %(host_id)s
        AND sproc_host_id = %(host_id)s
        AND sp_timestamp > current_date - interval '7 days'
        AND sp_timestamp < current_date
      WINDOW w AS (
        PARTITION BY sp_sproc_id ORDER BY sp_timestamp
      )
    ) a
    WHERE
      total_time - total_time_lag >= 0
      AND calls - calls_lag > 0
    GROUP BY
      sproc_name, sp_timestamp::date
),
q_sproc_avg_runtimes AS (
    SELECT
      sproc_name,
      avg(coalesce(runtime_per_day, 0)) AS avg_daily_runtime
   FROM
     q_sproc_total_runtimes_per_day
   GROUP BY
     sproc_name
),
q_avg_daily_total_runtime AS (
    SELECT
      avg(host_total_runtime)::int8 as avg_daily_total_runtime
    FROM (
        SELECT
          sp_timestamp::date,
          sum(runtime_per_day) as host_total_runtime
        FROM
          q_sproc_total_runtimes_per_day
        GROUP BY
          sp_timestamp::date
        ORDER BY
          sp_timestamp::date
    ) a
),
q_filter_condition AS (
    SELECT
      %(host_id)s AS host_id,
      'sproc_runtime'::text AS metric,
      sproc_name::text AS ident
      /*
      avg_daily_runtime::int8,
      avg_daily_runtime / (select avg_daily_total_runtime from q_avg_daily_total_runtime) * 100 as daily_pct
      */
    FROM
      q_sproc_avg_runtimes
    WHERE
      (avg_daily_runtime / (select avg_daily_total_runtime from q_avg_daily_total_runtime)) >= 0.005
    ORDER BY
      1
),
q_delete_expired AS (
    DELETE FROM
        olad.metric_ident_filter
    WHERE
        mif_host_id = %(host_id)s
        AND mif_metric = 'sproc_runtime'
        AND NOT EXISTS (
          select 1 from q_filter_condition
          where (q_filter_condition.host_id, q_filter_condition.metric, q_filter_condition.ident) = (mif_host_id, mif_metric, mif_ident)
        )
        AND NOT mif_is_permanent
    RETURNING *
),
q_insert_new AS (
    INSERT INTO olad.metric_ident_filter (mif_host_id, mif_metric, mif_ident)
    SELECT * FROM q_filter_condition
    WHERE NOT EXISTS (
      select 1 from olad.metric_ident_filter
      where (mif_host_id, mif_metric, mif_ident) = (q_filter_condition.host_id, q_filter_condition.metric, q_filter_condition.ident)
    )
    RETURNING *
)
SELECT
--    (select coalesce(count(1), 0) FROM q_delete_expired) as delete_count,
--    (select coalesce(count(1), 0) FROM q_insert_new) as insert_count
    mif_ident AS ident
FROM
    olad.metric_ident_filter
WHERE
    (mif_host_id, mif_metric) = (%(host_id)s, 'sproc_runtime')
UNION
SELECT
    mif_ident
FROM q_insert_new
EXCEPT
SELECT
    mif_ident
FROM
    q_delete_expired
ORDER BY
    1