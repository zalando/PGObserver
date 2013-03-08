'''
Created on Sep 20, 2011

@author: jmussler
'''
from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import datetime
import DataDB
import funccache
import collections

def viewSprocs(interval="AND sp_timestamp > ('now'::timestamp - '2 days'::interval)"):
    sql = """
    SELECT sproc_performance_data.sp_timestamp,
           sproc_performance_data.sp_sproc_id,
           COALESCE(sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER w, 0::bigint) AS delta_calls,
           COALESCE(sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER w, 0::bigint) AS delta_self_time,
           COALESCE(sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER w, 0::bigint) AS delta_total_time
      FROM monitor_data.sproc_performance_data
     WHERE TRUE
       """ + interval + """
    WINDOW w AS (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)
     ORDER BY sproc_performance_data.sp_timestamp
    """
    return sql

def getSprocsOrderedBy( hostId, order = " ORDER BY SUM(delta_total_time) DESC"):
    sql = """SELECT sproc_name
               FROM ( """ + viewSprocs() + """ ) t JOIN monitor_data.sprocs ON sp_sproc_id = sproc_id
               WHERE sproc_host_id = """ + str(hostId) + """
               GROUP BY sproc_name
             """ + order + """;
          """

    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    list= []
    cur.execute( sql )

    for r in cur:
        list.append ( r['sproc_name'] )

    cur.close()
    DataDB.closeDataConnection(conn)
    return list

def getSingleSprocSQL(name, hostId = 1, interval=None, sprocNr = None):

    if name[-1:]!=")":
        name = name + "("

    if(interval==None):
        interval = "AND sp_timestamp > ('now'::timestamp-'23 days'::interval)"
    else:
        if 'interval' in interval:
          interval = "AND sp_timestamp > " + interval
        else:
          interval = "AND sp_timestamp BETWEEN '%s'::timestamp AND '%s'::timestamp" % ( interval['from'], interval['to'], )

    if sprocNr == None:
        nameSql = """'"""+name+"""%'"""
    else:
        nameSql = """( SELECT DISTINCT sproc_name FROM monitor_data.sprocs sp WHERE sp.sproc_name LIKE '"""+name+"""%' AND sp.sproc_host_id = """ + str(hostId) + """ ORDER BY sproc_name ASC LIMIT 1 OFFSET """ + str(sprocNr) + """)"""

    sql = """SELECT ( SELECT sprocs.sproc_name
                        FROM monitor_data.sprocs
                       WHERE sprocs.sproc_id = t.sp_sproc_id) AS name,
          date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis,
          sum(t.delta_calls) AS d_calls,
          sum(t.delta_self_time) AS d_self_time,
          sum(t.delta_total_time) AS d_total_time,
          CASE WHEN sum(t.delta_calls) > 0 THEN sum(t.delta_total_time) / sum(t.delta_calls) ELSE 0 END AS d_avg_time,
          CASE WHEN sum(t.delta_calls) > 0 THEN sum(t.delta_self_time) / sum(t.delta_calls) ELSE 0 END AS d_avg_self_time
   FROM ( SELECT sproc_performance_data.sp_timestamp,
                 sproc_performance_data.sp_sproc_id,
                COALESCE(sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_calls,
                COALESCE(sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_self_time,
                COALESCE(sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_total_time
           FROM monitor_data.sproc_performance_data
          WHERE (sproc_performance_data.sp_sproc_id IN ( SELECT sprocs.sproc_id
                   FROM monitor_data.sprocs WHERE sproc_name LIKE """ + nameSql + """  AND sproc_host_id = """+str(hostId)+""" ))
            """ + interval + """
          ORDER BY sproc_performance_data.sp_timestamp) t
          GROUP BY t.sp_sproc_id, date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
          ORDER BY date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval"""

    return sql;

def getSingleSprocData(name, hostId=1, interval=None, sprocNr = None):
    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute( getSingleSprocSQL(name, hostId, interval, sprocNr ) )

    data = { 'calls' : [], 'self_time': [], 'total_time' : [] , 'avg_time' : [] , 'avg_self_time': [] , 'name' : name }

    for r in cur:
        data['calls'].append( ( r['xaxis'] , r['d_calls'] ) )
        data['total_time'].append ( ( r['xaxis'] , r['d_total_time'] ) )
        data['self_time'].append ( ( r['xaxis'] , r['d_self_time'] ) )
        data['avg_time'].append ( ( r['xaxis'] , r['d_avg_time'] ) )
        data['avg_self_time'].append ( ( r['xaxis'] , r['d_avg_self_time'] ) )

    cur.close()
    DataDB.closeDataConnection(conn)

    return data

def getAllSprocs():
    pass

def getSprocDataByTags():
    sql = """select tm_tag_id , sum("yaxis") AS "yaxis_t" , sum("yaxis2") AS "yaxis_c", "xaxis"  from (
 select group_date(sp_timestamp,30) as "xaxis",
        sp_sproc_id,
        max(sp_self_time) - min(sp_self_time) as "yaxis",
        max(sp_calls) - min(sp_calls) as "yaxis2"
   from monitor_data.sproc_performance_data
  where sp_timestamp > 'now'::timestamp - '9 days'::interval
  group by sp_sproc_id , group_date(sp_timestamp,30) ) data,
  monitor_data.sprocs,
  monitor_data.tag_members
  where sprocs.sproc_id = sp_sproc_id
    and tm_sproc_name = sproc_name
    and tm_schema = get_noversion_name(sproc_schema)
  group by tm_tag_id , "xaxis" order by 4 asc;"""


    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(sql)

    data = collections.defaultdict(list)

    for r in cur:
        data[r['tm_tag_id']].append((r['xaxis'], r['yaxis_t'], r['yaxis_c']))

    cur.close()
    DataDB.closeDataConnection(conn)

    return data

