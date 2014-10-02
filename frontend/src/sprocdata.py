from __future__ import print_function
import psycopg2
import psycopg2.extras
import datadb
from psycopg2.extensions import adapt


def viewSprocs(hostId, interval="AND sp_timestamp > ('now'::timestamp - '2 days'::interval)"):
    sql = """
    SELECT sproc_performance_data.sp_timestamp,
           sproc_performance_data.sp_sproc_id,
           COALESCE(sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER w, 0::bigint) AS delta_calls,
           COALESCE(sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER w, 0::bigint) AS delta_self_time,
           COALESCE(sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER w, 0::bigint) AS delta_total_time
      FROM monitor_data.sproc_performance_data
     WHERE sp_host_id = """ + str(adapt(hostId)) + """
       """ + interval + """
    WINDOW w AS (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)
     ORDER BY sproc_performance_data.sp_timestamp
    """
    return sql

def getActiveSprocsOrderedBy( hostId, order = " ORDER BY SUM(delta_total_time) DESC"):
    sql = """SELECT sproc_name
               FROM ( """ + viewSprocs(hostId) + """ ) t JOIN monitor_data.sprocs ON sp_sproc_id = sproc_id
               WHERE sproc_host_id = """ + str(adapt(hostId)) + """
               GROUP BY sproc_name
             """ + order + """;
          """

    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    list= []
    cur.execute( sql )

    for r in cur:
        list.append ( r['sproc_name'] )

    cur.close()
    datadb.closeDataConnection(conn)
    return list

def getSingleSprocSQL(hostId, name, interval=None):

    if name[-1:]!=")":
        name = name + "("

    if(interval==None):
        interval = "AND sp_timestamp > ('now'::timestamp-'23 days'::interval)"
    else:
        if 'interval' in interval:
          interval = "AND sp_timestamp > " + interval
        else:
          interval = "AND sp_timestamp BETWEEN %s::timestamp AND %s::timestamp" % ( adapt(interval['from']), adapt(interval['to']), )

    nameSql = str(adapt(name+'%'))

    sql = """SELECT ( SELECT sprocs.sproc_name
                        FROM monitor_data.sprocs
                       WHERE sprocs.sproc_id = t.sp_sproc_id) AS name,
          date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis,
          sum(t.delta_calls) AS d_calls,
          sum(t.delta_self_time) AS d_self_time,
          sum(t.delta_total_time) AS d_total_time,
          CASE WHEN sum(t.delta_calls) > 0 THEN round(sum(t.delta_total_time) / sum(t.delta_calls), 2) ELSE 0 END AS d_avg_time,
          CASE WHEN sum(t.delta_calls) > 0 THEN round(sum(t.delta_self_time) / sum(t.delta_calls), 2) ELSE 0 END AS d_avg_self_time
   FROM ( SELECT sproc_performance_data.sp_timestamp,
                 sproc_performance_data.sp_sproc_id,
                COALESCE(sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_calls,
                COALESCE(sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_self_time,
                COALESCE(sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp), 0::bigint) AS delta_total_time
           FROM monitor_data.sproc_performance_data
          WHERE (sproc_performance_data.sp_sproc_id IN ( SELECT sprocs.sproc_id
                   FROM monitor_data.sprocs WHERE sproc_name LIKE """ + nameSql + """  AND sproc_host_id = """+ str(adapt(hostId))+""" ))
            """ + interval + """
            AND sproc_performance_data.sp_host_id = """  + str(adapt(hostId)) + """
          ORDER BY sproc_performance_data.sp_timestamp) t
          GROUP BY t.sp_sproc_id, date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
          ORDER BY date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval"""

    return sql

def getSingleSprocData(hostId, name, interval=None):
    data = { 'calls' : [], 'self_time': [], 'total_time' : [] , 'avg_time' : [] , 'avg_self_time': [] , 'name' : None }
    if name:
        dataset = datadb.execute( getSingleSprocSQL(hostId, name, interval ) )
        for r in dataset:
            if not data['name']:
                data['name'] = r['name']    # in case sproc is present in multiple apis the name from 1st api will be used
            data['calls'].append( ( r['xaxis'] , r['d_calls'] ) )
            data['total_time'].append ( ( r['xaxis'] , r['d_total_time'] ) )
            data['self_time'].append ( ( r['xaxis'] , r['d_self_time'] ) )
            data['avg_time'].append ( ( r['xaxis'] , r['d_avg_time'] ) )
            data['avg_self_time'].append ( ( r['xaxis'] , r['d_avg_self_time'] ) )

    return data


def getAllActiveSprocNames(hostId):
    sql = """
    select
      distinct regexp_replace(sproc_name,'(\(.*\))','') as sproc_name
    from
      sprocs
      join sproc_performance_data on sp_sproc_id = sproc_id
    where sproc_host_id = %s
      and sp_host_id = %s
      and sp_timestamp > now() - '1 day'::interval
    """
    ret = datadb.execute(sql, (hostId, hostId))
    ret = [ x['sproc_name'] for x in ret ]
    ret.sort()
    return ret


if __name__ == '__main__':
    datadb.setConnectionString('dbname=prod_pgobserver_db host=pgobserver.db port=5433 user=kmoppel')
    print (getAllActiveSprocNames())

