from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import DataDB
import funccache
from psycopg2.extensions import adapt

'''
Created on 17.09.2011

@author: Jan
'''

""" Query for seqscan

SELECT "name",max("size"),max("delta") FROM (
SELECT ( SELECT t_schema||'.'||t_name FROM monitor_data.tables WHERE t_id = tsd_table_id ) AS "name",
       MAX(tsd_seq_scans) - MIN(tsd_seq_scans) AS delta,
       MAX(tsd_table_size) AS "size"
  FROM monitor_data.table_size_data WHERE tsd_timestamp > ( 'now'::timestamp - '3 days'::interval ) AND tsd_table_id IN ( SELECT t_id FROM monitor_data.tables WHERE t_host_id = 1 )
  GROUP BY tsd_table_id , tsd_timestamp::date ) t WHERE t.delta > (12*24) GROUP BY "name" ORDER BY 2 DESC


"""


def makeTimeIntervalReadable(micro):
    s = int(micro/1000)
    micro %= 1000
    m  = int(s/60)
    s %= 60
    h  = int(m/60)
    m %= 60

    if h > 0:
        return str(h) + "h "+ str(m) + "m "+str(s) +"s"

    if m > 0:
        return str(m) + "m "+str(s)+"."+ '{0:03}'.format(micro) + "s"

    return str(s)+"." + '{0:03}'.format(micro) + "s"

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
                      WHERE (sproc_performance_data.sp_sproc_id IN ( SELECT sprocs.sproc_id
                                                                       FROM monitor_data.sprocs WHERE sproc_host_id = """ + str(adapt(hostId)) + """ ))
                                                                       """ + interval + """
                          WINDOW w AS ( PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp )
                          ORDER BY sproc_performance_data.sp_timestamp) t
              GROUP BY t.sp_sproc_id, date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
              ORDER BY date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval"""
    return sql;

@funccache.lru_cache(60,25)
def getTop10Interval(order=avgRuntimeOrder,interval=None,hostId = 1, limit = 10):

    sql = """select regexp_replace("name", E'(\\\\(.*\\\\))','()') AS "name",
                    round( sum(d_calls) , 0 ) AS "calls",
                    round( sum(d_total_time) , 0 ) AS "totalTime",
                    round( sum(d_total_time) / sum(d_calls) , 0 ) AS "avgTime"
               from ( """ + getSQL(interval, hostId) + """) tt
              where d_calls > 0
              group by "name"
              order by """+order+"""  limit """ + str(adapt(limit))

    conn = DataDB.getDataConnection()
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

@funccache.lru_cache(60, 1)
def getLoad(hostId=1):
    sql = """select xaxis, sum(d_self_time) OVER (ORDER BY xaxis ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) / (1*15*60*1000) AS load_15min,
                           sum(d_self_time) OVER (ORDER BY xaxis ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) / (4*15*60*1000) AS load_1hour
               from ( select xaxis,sum(d_self_time) d_self_time from (""" + getSQL("('now'::timestamp - '9 days'::interval)" ,hostId) + """) dataTabel group by xaxis ) loadTable """

    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    load = { 'load_15min' : [], 'load_1hour': [] }

    cur.execute(sql)
    lastTime = None
    skip15min=0
    skip1h=0

    for record in cur:
        currentTime = int(time.mktime(record['xaxis'].timetuple()) * 1000)
        if lastTime != None:
            if currentTime - lastTime > ( 15 * 60 * 1000):
                skip15min = 2
                skip1h=5

        if skip15min>0:
            skip15min -= 1
        else:
            load['load_15min'].append((record['xaxis'], round ( record['load_15min'], 2 ) ) )

        if skip1h > 0:
            skip1h -= 1
        else:
            load['load_1hour'].append((record['xaxis'], round ( record['load_1hour'] , 2 )))

        lastTime = int(time.mktime(record['xaxis'].timetuple()) * 1000)

    cur.close()
    conn.close()

    return load

def getCpuLoad(hostId=1):
    load = { "load_15min_avg" : [] , "load_15min_max" : [] }

    sql = """ SELECT date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval AS load_timestamp,
                     AVG(load_1min_value) AS load_15min_avg,
                     MAX(load_1min_value) AS load_15min_max
                FROM monitor_data.host_load WHERE load_host_id = """ + str(adapt(hostId)) + """ AND load_timestamp > ('now'::timestamp - '9 days'::interval)
                GROUP BY date_trunc('hour'::text, load_timestamp) + floor(date_part('minute'::text, load_timestamp) / 15::double precision) * '00:15:00'::interval
                ORDER BY 1 ASC """

    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(sql)
    for record in cur:
        load['load_15min_avg'].append( (record['load_timestamp'] , round( float(record['load_15min_avg'])/100,2) ) )
        load['load_15min_max'].append( (record['load_timestamp'] , round( float(record['load_15min_max'])/100,2) ) )

    return load

def getSprocLoad(hours,averageInterval):
    return []

def getSproc(name):
    """SELECT (SELECT sproc_name FROM monitor_data.sprocs WHERE sproc_id = sp_sproc_id) AS "name",
       date_trunc('hour', sp_timestamp)::timestamp +  ( floor(extract('minute' from sp_timestamp) / 15)*'15 minutes'::interval ) AS "xaxis",
       SUM("delta_calls") AS "d_calls",
       SUM("delta_self_time") AS "d_self_time",
       SUM("delta_total_time") AS "d_total_time"
  FROM
 ( SELECT sp_timestamp,sp_sproc_id,
       COALESCE( sp_calls - lag(sp_calls) OVER ( PARTITION BY sp_sproc_id ORDER BY sp_timestamp), 0 ) AS "delta_calls",
       COALESCE( sp_self_time - lag(sp_self_time) OVER ( PARTITION BY sp_sproc_id ORDER BY sp_timestamp), 0 ) AS "delta_self_time",
       COALESCE( sp_total_time- lag(sp_total_time) OVER ( PARTITION BY sp_sproc_id ORDER BY sp_timestamp), 0 ) AS "delta_total_time"
FROM monitor_data.sproc_performance_data WHERE sp_sproc_id IN ( SELECT sproc_id FROM monitor_data.sprocs WHERE sproc_name LIKE ANY(ARRAY['%"""+str(adapt(name))+"""%'])) ORDER BY sp_timestamp ) t
GROUP BY sp_sproc_id, date_trunc('hour', sp_timestamp)::timestamp +  ( floor(extract('minute' from sp_timestamp) / 15)*'15 minutes'::interval ) ORDER BY "xaxis" """
    pass

