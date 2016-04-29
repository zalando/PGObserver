SELECT
  load_timestamp AS dt,
  load_15min_value AS value,
  'load_5' as ident
FROM
  monitor_data.host_load
WHERE
  load_host_id = %(host_id)s
  AND load_timestamp > %(date_from)s
  AND (%(date_to)s IS NULL OR load_timestamp < %(date_to)s)
ORDER BY
  ident, dt
