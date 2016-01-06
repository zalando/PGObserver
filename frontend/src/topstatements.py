import itertools
import topsprocs
import datadb


TOP_STATEMENTS_SQL = """
WITH q_data AS (
  SELECT
    *,
    case
      when length(ssd_query_full) > 55 then ssd_query_full::varchar(55)||'..'
      else ssd_query_full
    end as ssd_query
  FROM (
    SELECT
      ssd_timestamp,
      ssd_query_id,
      lower(ltrim(regexp_replace(ssd_query, E'[ \\t\\n\\r]+' , ' ', 'g'))) as ssd_query_full,
      ssd_total_time,
      ssd_calls
    FROM
      monitor_data.stat_statements_data
    WHERE
      ssd_host_id = %(host_id)s
      AND ssd_timestamp > now() - %(interval1)s::interval
  ) ssd
), q_agg_int1 AS (
    SELECT
      ssd_query as query,
      ssd_query_id as query_id,
      calls,
      total_ms,
      round(total_ms / calls::numeric, 3) as avg_ms
    FROM (
        SELECT
          ssd_query_id,
          ssd_query,
          max(ssd_total_time) - min(ssd_total_time) as total_ms,
          max(ssd_calls) - min(ssd_calls) as calls
        FROM
          q_data
        GROUP BY
          ssd_query_id, ssd_query
    ) a
    WHERE
      calls > 0 AND total_ms > 0
), q_agg_int2 AS (
    SELECT
      ssd_query as query,
      ssd_query_id as query_id,
      calls,
      total_ms,
      round(total_ms / calls::numeric, 3) as avg_ms
    FROM (
        SELECT
          ssd_query_id,
          ssd_query,
          max(ssd_total_time) - min(ssd_total_time) as total_ms,
          max(ssd_calls) - min(ssd_calls) as calls
        FROM
          q_data
        WHERE
          ssd_timestamp > now() - %(interval2)s::interval
        GROUP BY
          ssd_query_id, ssd_query
    ) a
    WHERE
      calls > 0 AND total_ms > 0
), q_calls_int1 AS (
    SELECT
      'calls_int1'::text as mode,
      *
    FROM
      q_agg_int1
    ORDER BY
      calls DESC
    LIMIT
      %(limit)s
), q_calls_int2 AS (
    SELECT
      'calls_int2'::text as mode,
      *
    FROM
      q_agg_int2
    WHERE
      calls > 0 AND total_ms > 0
    ORDER BY
      calls DESC
    LIMIT
      %(limit)s
), q_avg_int1 AS (
    SELECT
      'avg_int1'::text as mode,
      *
    FROM
      q_agg_int1
    WHERE
      calls > 0 AND total_ms > 0
    ORDER BY
      avg_ms DESC
    LIMIT
      %(limit)s
), q_avg_int2 AS (
    SELECT
      'avg_int2'::text as mode,
      *
    FROM
      q_agg_int2
    WHERE
      calls > 0 AND total_ms > 0
    ORDER BY
      avg_ms DESC
    LIMIT
      %(limit)s
), q_total_int1 AS (
    SELECT
      'total_int1'::text as mode,
      *
    FROM
      q_agg_int1
    WHERE
      calls > 0 AND total_ms > 0
    ORDER BY
      total_ms DESC
    LIMIT
      %(limit)s
), q_total_int2 AS (
    SELECT
      'total_int2'::text as mode,
      *
    FROM
      q_agg_int2
    WHERE
      calls > 0 AND total_ms > 0
    ORDER BY
      total_ms DESC
    LIMIT
      %(limit)s
)
SELECT
  *
FROM
 q_calls_int1
UNION ALL
SELECT
  *
FROM
 q_calls_int2
UNION ALL
SELECT
  *
FROM
 q_avg_int1
UNION ALL
SELECT
  *
FROM
 q_avg_int2
UNION ALL
SELECT
  *
FROM
 q_total_int1
UNION ALL
SELECT
  *
FROM
 q_total_int2
"""


def getTopStatementsData(hostId, interval1='3hours', interval2='1hour', limit='10'):
    data = datadb.execute(TOP_STATEMENTS_SQL, {'host_id': hostId,
                                               'interval1': interval1, 'interval2': interval2,
                                               'limit': limit})
    for d in data:
        d['avg_time_pretty'] = topsprocs.makeTimeIntervalReadable(d['avg_ms'])
        d['total_time_pretty'] = topsprocs.makeTimeIntervalReadable(d['total_ms'])

    return {x: list(y) for x, y in itertools.groupby(data, lambda x: x['mode'])}
