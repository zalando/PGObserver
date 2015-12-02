from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import datadb
import tplE
from psycopg2.extensions import adapt


def makeTimeIntervalReadable(total_millis):
    total_s = int(total_millis) / 1000.0
    s = int(total_millis / 1000)
    m = int(s/60)
    h = int(m/60)

    if total_millis < 1:
        return str(int(total_millis * 1000)) + "us"
    if s == 0:
        return str(int(total_millis)) + "ms"
    if m == 0:
        return '{0:.2f}'.format(total_s) + "s"
    if h == 0:
        return str(m) + "m " + '{0:.0f}'.format(total_s % 60) + "s"
    return str(h) + "h " + str(m) + "m " + str(s) + "s"


avgRuntimeOrder = "sum(d_total_time) / sum(d_calls) desc"
totalRuntimeOrder = "sum(d_total_time) desc"
totalCallsOrder = "sum(d_calls) desc"


def getSQL(interval=None,hostId = 1):

    if(interval==None):
        interval = ""
    else:
        interval = "AND sp_timestamp > " + interval

    sql = """SELECT ( SELECT sprocs.sproc_name
               FROM monitor_data.sprocs
              WHERE sprocs.sproc_id = t.sp_sproc_id) AS name, date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis, sum(t.delta_calls) AS d_calls, sum(t.delta_self_time) AS d_self_time, sum(t.delta_total_time) AS d_total_time
               FROM ( SELECT sproc_performance_data.sp_timestamp,
                             sproc_performance_data.sp_sproc_id,
                            COALESCE(sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER w, 0::bigint) AS delta_calls,
                            COALESCE(sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER w, 0::bigint) AS delta_self_time,
                            COALESCE(sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER w, 0::bigint) AS delta_total_time
                       FROM monitor_data.sproc_performance_data
                      WHERE sproc_performance_data.sp_host_id = """ + str(adapt(hostId)) + """
                       """ + interval + """
                          WINDOW w AS ( PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp )
                          ORDER BY sproc_performance_data.sp_timestamp) t
              GROUP BY t.sp_sproc_id, date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
              ORDER BY date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval"""
    return sql;

#@funccache.lru_cache(60,25)
def getTop10Interval(order=avgRuntimeOrder,interval=None,hostId = 1, limit = 10):

    sql = """select regexp_replace("name", E'(\\\\(.*\\\\))','()') AS "name",
                    round( sum(d_calls) , 0 ) AS "calls",
                    round( sum(d_total_time) , 0 ) AS "totalTime",
                    round( sum(d_total_time) / sum(d_calls) , 0 ) AS "avgTime"
               from ( """ + getSQL(interval, hostId) + """) tt
              where d_calls > 0
              group by "name"
              order by """+order+"""  limit """ + str(adapt(limit))

    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(sql)

    sprocs = []

    for record in cur:
        record['avgTime'] = makeTimeIntervalReadable(record['avgTime'])
        record['totalTime'] = makeTimeIntervalReadable(record['totalTime'])
        sprocs.append(record)

    conn.close()

    return sprocs

def getTop10AllTimes(order, hostId = 1):
    return getTop10Interval(order)

def getTop10LastXHours(order,hours=1, hostId = 1, limit = 10):
    return getTop10Interval(order,"('now'::timestamp- %s::interval)" % ( adapt("%s hours" % ( hours, )), ), hostId , limit )

def getLoad(hostId, days='8'):
    days += 'days'
    sql = """
        SELECT
          xaxis,
          MAX(load_15min) as load_15min -- needed for 15min overlap
        FROM (
            SELECT
              sla_timestamp as xaxis,
              sla_load_15min as load_15min
            FROM
              monitor_data.sproc_load_agg
            WHERE
              sla_host_id = """ +str(adapt(hostId)) + """
              AND sla_timestamp > now() - """ + str(adapt(days)) + """::interval
              AND sla_timestamp < now() - '2 hours'::interval
            UNION ALL
              SELECT
                xaxis,
                (sum(d_self_time) OVER (ORDER BY xaxis ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) / (1*15*60*1000))::numeric(8,2) AS load_15min
              FROM
                ( SELECT
                    date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis,
                    sum(t.delta_self_time) AS d_self_time
                  FROM ( SELECT
                           spd.sp_timestamp,
                           COALESCE(spd.sp_self_time - lag(spd.sp_self_time) OVER w, 0::bigint) AS delta_self_time
                         FROM
                           monitor_data.sproc_performance_data spd
                         WHERE
                           spd.sp_host_id = """ + str(adapt(hostId)) + """
                           AND sp_timestamp >= now() - '2 hours 15 minutes'::interval --15 minutes overlap due to window
                         WINDOW w AS
                           ( PARTITION BY spd.sp_sproc_id ORDER BY spd.sp_timestamp )
                       ) t
                  GROUP BY
                    date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
                  ORDER BY
                    date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
                ) loadTable
            ) a
            GROUP BY
              xaxis
            ORDER BY
              xaxis
    """
    if not tplE._settings.get('run_aggregations'):
        sql = """
            SELECT
              xaxis,
              load_15min
            FROM (
                  SELECT
                    xaxis,
                    (sum(d_self_time) OVER (ORDER BY xaxis ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) / (1*15*60*1000))::numeric(8,2) AS load_15min
                  FROM
                    ( SELECT
                        date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis,
                        sum(t.delta_self_time) AS d_self_time
                      FROM ( SELECT
                               spd.sp_timestamp,
                               COALESCE(spd.sp_self_time - lag(spd.sp_self_time) OVER w, 0::bigint) AS delta_self_time
                             FROM
                               monitor_data.sproc_performance_data spd
                             WHERE
                               spd.sp_host_id = """ + str(adapt(hostId)) + """
                               AND sp_timestamp > now() - """ + str(adapt(days)) + """::interval
                             WINDOW w AS
                               ( PARTITION BY spd.sp_sproc_id ORDER BY spd.sp_timestamp )
                           ) t
                      GROUP BY
                        date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
                      ORDER BY
                        date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
                    ) loadTable
                ) a
                ORDER BY
                  xaxis
        """

    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    load = { 'load_15min' : [] }
    cur.execute(sql)
    lastTime = None
    skip15min=0

    for record in cur:
        currentTime = int(time.mktime(record['xaxis'].timetuple()) * 1000)
        if lastTime != None:
            if currentTime - lastTime > ( 15 * 60 * 1000):
                skip15min = 2

        if skip15min>0:
            skip15min -= 1
        else:
            load['load_15min'].append((record['xaxis'], round ( record['load_15min'], 2 ) ) )

        lastTime = int(time.mktime(record['xaxis'].timetuple()) * 1000)

    cur.close()
    conn.close()

    return load



def getCpuLoad(hostId, days='8'):
    load = { "load_15min_avg" : [] , "load_15min_max" : [] }
    days += 'days'
    sql = """ SELECT date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval AS load_timestamp,
                     AVG(load_1min_value) AS load_15min_avg,
                     MAX(load_1min_value) AS load_15min_max
                FROM monitor_data.host_load WHERE load_host_id = """ + str(adapt(hostId)) + """ AND load_timestamp > now() - """ + str(adapt(days)) + """::interval
                GROUP BY date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval
                ORDER BY 1 ASC """

    for record in datadb.execute(sql):
        load['load_15min_avg'].append( (record['load_timestamp'] , round( float(record['load_15min_avg'])/100,2) ) )
        load['load_15min_max'].append( (record['load_timestamp'] , round( float(record['load_15min_max'])/100,2) ) )

    return load

# TODO merge with cpuload
def getWalVolumes(hostId, days='8'):
    load = { "wal_15min_growth" : []}
    days += 'days'

    sql = """
            SELECT 
                date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval AS load_timestamp,
                coalesce(max(xlog_location_mb)-min(xlog_location_mb),0)  AS wal_15min_growth
            FROM monitor_data.host_load WHERE load_host_id = """ + str(adapt(hostId)) + """ AND load_timestamp > ('now'::timestamp - """ + str(adapt(days)) + """::interval)
            GROUP BY date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval
            ORDER BY 1 ASC
            """

    for record in datadb.execute(sql):
        load['wal_15min_growth'].append( (record['load_timestamp'] , record['wal_15min_growth'] ) )

    return load

def getBlockedProcessesCounts(hostId, days='8'):
    ret = []
    days += 'days'

    sql = """
with
q_wait_startpoints as (
     select
       date_trunc('hour'::text, query_start) + floor(date_part('minute'::text, query_start) / 15::double precision) * '00:15:00'::interval as wait_start_timestamp,
       count(1) as wait_starts
     from
     (
          select
            query_start,
            query,
            min(bp_timestamp) as wait_start,
            max(bp_timestamp) as wait_end,
            count(1)
          from
            monitor_data.blocking_processes
          where
            bp_host_id = """ + str(adapt(hostId)) + """
            and bp_timestamp > now() - """ + str(adapt(days)) + """::interval
            and waiting
          group by
            1, 2
          order by
            1, 2
     ) a
     where
       wait_end - wait_start >= '5 seconds'::interval
     group by
       date_trunc('hour'::text, query_start) + floor(date_part('minute'::text, query_start) / 15::double precision) * '00:15:00'::interval
),
q_timeline as (
     select * from (select generate_series(current_date - """ + str(adapt(days)) + """::interval, now(), '00:15:00'::interval) AS ts) a
     where ts > now() - """ + str(adapt(days)) + """::interval
)
SELECT
  date_trunc('hour'::text, q_timeline.ts) + floor(date_part('minute'::text, q_timeline.ts) / 15::double precision) * '00:15:00'::interval AS ts,
  coalesce(wait_starts, 0) as count
FROM
  q_timeline
  left join
  q_wait_startpoints on q_wait_startpoints.wait_start_timestamp = q_timeline.ts
ORDER BY
  1 ASC
            """

    for record in datadb.execute(sql):
        ret.append( (record['ts'] , record['count'] ) )

    return ret


