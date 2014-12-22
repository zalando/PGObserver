from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import datadb
import tplE
from psycopg2.extensions import adapt


def getSizeTrendSQL(host_id, days='8'):
    days += 'days'
    sql = """
        SELECT
          tsda_timestamp AS tsd_timestamp,
          """ + str(adapt(host_id)) + """ AS tsd_host_id,
          tsda_db_size AS size,
          tsda_tup_ins AS s_ins,
          tsda_tup_upd AS s_upd,
          tsda_tup_del AS s_del
        FROM
          monitor_data.table_size_data_agg
        WHERE
          tsda_timestamp > now() - """ + str(adapt(days)) + """::interval
          AND tsda_timestamp < now() - '2 hours'::interval
          AND tsda_host_id = """ + str(adapt(host_id)) + """
        UNION ALL
        SELECT
          *
        FROM
          ( SELECT
              tsd_timestamp,
              """ + str(adapt(host_id)) + """ AS tsd_host_id,
              SUM(tsd_table_size)+SUM(tsd_index_size) AS size,
              SUM(tsd_tup_ins) AS s_ins,
              SUM(tsd_tup_upd) AS s_upd,
              SUM(tsd_tup_del) AS s_del
            FROM
              monitor_data.table_size_data
            WHERE
              tsd_timestamp >= now() - '2 hours'::interval
              AND tsd_host_id = """ + str(adapt(host_id)) + """
            GROUP BY
              tsd_timestamp
            ORDER BY
              tsd_timestamp
          ) a
        """
    if not tplE._settings['run_aggregations']:
        sql = """SELECT
                    tsd_timestamp,
                    """ + str(adapt(host_id)) + """ AS tsd_host_id,
                    ( SUM(tsd_table_size)+SUM(tsd_index_size) ) AS size,
                    SUM(tsd_tup_ins) AS s_ins,
                    SUM(tsd_tup_upd) AS s_upd,
                    SUM(tsd_tup_del) AS s_del
                FROM monitor_data.table_size_data
                WHERE tsd_timestamp > 'now'::timestamp - """ + str(adapt(days)) + """::interval
                AND tsd_host_id = """ + str(adapt(host_id)) + """
              GROUP BY tsd_timestamp
              ORDER BY tsd_timestamp
              """

    return sql



def getDatabaseSizes(host_id = None, days='8'):
    size_data = {}
    current_host = 0

    for record in datadb.execute(getSizeTrendSQL(host_id, days)):

        if record['tsd_host_id'] != current_host:
            current_host = record['tsd_host_id']
            set_ins = False
            set_del = False
            set_upd = False

            l_ins = None
            l_upd = None
            l_del = None

        if not record['tsd_host_id'] in size_data:
            size_data[record['tsd_host_id']] = { 'size' : [] , 'ins': [], 'upd': [], 'del':[] }

        """ exclude 0 values, otherwise there is a big peak at start, with wraparound this should be ok"""

        if not set_ins and record['s_ins']!=0:
            l_ins = record['s_ins']
            set_ins = True

        if not set_upd and record['s_upd']!=0:
            l_upd = record['s_upd']
            set_upd = True

        if not set_del and record['s_del']!=0:
            l_del = record['s_del']
            set_del = True

        if l_ins == None:
            l_ins = record['s_ins']

        if l_upd == None:
            l_upd = record['s_upd']

        if l_del == None:
            l_del = record['s_del']

        size_data[record['tsd_host_id']]['size'].append( ( record['tsd_timestamp'] , record['size'] ) )
        size_data[record['tsd_host_id']]['ins'].append( ( record['tsd_timestamp'] , max( record['s_ins'] - l_ins , 0)  ) )
        size_data[record['tsd_host_id']]['del'].append( ( record['tsd_timestamp'] , max( record['s_del'] - l_del , 0)  ) )
        size_data[record['tsd_host_id']]['upd'].append( ( record['tsd_timestamp'] , max( record['s_upd'] - l_upd , 0)  ) )

        l_ins = record['s_ins']
        l_upd = record['s_upd']
        l_del = record['s_del']

    return size_data

def makePrettySize(size):
    mb = int(round( size / 1048576.0))
    return str(mb) + " MB"

def makePrettyCounter(count):
    if count <= 1000:
        return str(count)
    if count < 1000000:
        return str(round(count / 1000.0, 1)) + ' k'
    return str(round(count / 1000000.0, 1)) + ' M'

def getSingleTableSql(host, name, interval=None):
    if interval==None:
        interval = "AND tsd_timestamp > ('now'::timestamp - '8 days'::interval)"
    else:
        if 'interval' in interval:
            interval = "AND tsd_timestamp > %s::interval" % (adapt(interval['interval']), )
        else:
            interval = "AND tsd_timestamp BETWEEN %s::timestamp and %s::timestamp" % (adapt(interval['from']),adapt(interval['to']), )

    sql = """
    SELECT tsd_table_id,
           tsd_timestamp,
           tsd_table_size,
           tsd_index_size,
           tsd_seq_scans,
           tsd_index_scans,
           tsd_tup_ins,
           tsd_tup_upd,
           tsd_tup_del,
           tsd_tup_hot_upd
      FROM monitor_data.table_size_data
     WHERE tsd_table_id = ( SELECT t_id FROM monitor_data.tables WHERE t_schema || '.' || t_name = """ + str(adapt(name)) + """ AND t_host_id = """ + str(adapt(host)) + """ )
       AND tsd_host_id = """ + str(adapt(host)) + """
       """+interval+"""
      ORDER BY tsd_timestamp ASC
    """

    return sql

def getSingleTableIOSql(host, name, interval=None):

    if interval==None:
        interval = "AND tio_timestamp > ('now'::timestamp - '8 days'::interval)"
    else:
        if 'interval' in interval:
            interval = "AND tio_timestamp > %s::interval" % ( adapt(interval['interval']), )
        else:
            interval = "AND tio_timestamp BETWEEN %s::timestamp and %s::timestamp" % (adapt(interval['from']),adapt(interval['to']), )

    sql = """
    SELECT tio_table_id, tio_timestamp, tio_heap_read, tio_heap_hit, tio_idx_read,
           tio_idx_hit
      FROM monitor_data.table_io_data
     WHERE tio_table_id = ( SELECT t_id FROM monitor_data.tables WHERE t_schema || '.' || t_name = """ + str(adapt(name)) + """ AND t_host_id = """ + str(adapt(host)) + """ )
       AND tio_host_id = """ + str(adapt(host)) + """
       """+interval+"""
      ORDER BY tio_timestamp ASC
    """

    return sql

def getTableIOData(host, name, interval = None):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(getSingleTableIOSql(host,name,interval))

    d = { 'heap_read' : [], 'heap_hit' : [], 'index_read' : [], 'index_hit': [] }

    last_hr = None
    last_hh = None
    last_ir = None
    last_ih = None

    for r in cur:

        if last_hr != None:
            d['heap_read'].append(( r['tio_timestamp'] , r['tio_heap_read'] - last_hr ))

        if last_hh != None:
            d['heap_hit'].append(( r['tio_timestamp'] , r['tio_heap_hit'] - last_hh ))

        if last_ir != None:
            d['index_read'].append(( r['tio_timestamp'] , r['tio_idx_read'] - last_ir ))

        if last_ih != None:
            d['index_hit'].append(( r['tio_timestamp'] , r['tio_idx_hit'] - last_ih ))

        last_hr = r['tio_heap_read']
        last_hh = r['tio_heap_hit']
        last_ir = r['tio_idx_read']
        last_ih = r['tio_idx_hit']

    cur.close()
    datadb.closeDataConnection(conn)

    return d


def getTableData(host, name, interval = None):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(getSingleTableSql(host,name,interval))

    d = { 'table_size' : [], 'index_size' : [], 'seq_scans': [], 'index_scans' : [], 'ins':[], 'upd':[], 'del':[], 'hot':[] }

    last_is = None
    last_ss = None

    last_ins = None
    last_del = None
    last_upd = None
    last_hot = None
    last_timestamp = 0

    for r in cur:
        d['table_size'].append ( ( r['tsd_timestamp'] , r['tsd_table_size'] ) )
        d['index_size'].append ( ( r['tsd_timestamp'] , r['tsd_index_size'] ) )

        if last_ss != None:
            d['seq_scans'].append  ( ( r['tsd_timestamp'] , r['tsd_seq_scans']-last_ss ) )

        if last_is != None:
            d['index_scans'].append( ( r['tsd_timestamp'] , r['tsd_index_scans'] - last_is ) )

        if last_ins != None and last_ins != 0:
            d['ins'].append( ( r['tsd_timestamp'] , r['tsd_tup_ins'] - last_ins ) )

        if last_del != None and last_del != 0:
            d['del'].append( ( r['tsd_timestamp'] , r['tsd_tup_del'] - last_del ) )

        if last_upd != None and last_upd != 0:
            d['upd'].append( ( r['tsd_timestamp'] , r['tsd_tup_upd'] - last_upd ) )

        if last_hot != None and last_hot != 0:
            d['hot'].append( ( r['tsd_timestamp'] , r['tsd_tup_hot_upd'] - last_hot ) )

        last_is = r['tsd_index_scans']
        last_ss = r['tsd_seq_scans']

        last_ins = r['tsd_tup_ins']
        last_del = r['tsd_tup_del']
        last_upd = r['tsd_tup_upd']
        last_hot = r['tsd_tup_hot_upd']

        last_timestamp = int(time.mktime(r['tsd_timestamp'].timetuple()) * 1000)

    cur.close()
    datadb.closeDataConnection(conn)

    return d


def getTopTables(hostId, date_from, date_to, order=2, limit=10):
    limit_sql = "" if limit is None else """ LIMIT """ + str(adapt(limit))

    order_by_sql = { 1: "ORDER BY schema ASC,name ASC ",
              2: "ORDER BY table_size DESC" ,
              3: "ORDER BY table_size - min_table_size DESC",
              4: "ORDER BY CASE WHEN min_table_size > 0 THEN table_size::float / min_table_size ELSE 0 END DESC",
              5: "ORDER BY index_size DESC",
              6: "ORDER BY index_size - min_index_size DESC",
              7: "ORDER BY CASE WHEN min_index_size > 0 THEN index_size::float / min_index_size ELSE 0 END DESC",
              8: "ORDER BY iud_delta DESC",
              9: "ORDER BY s_delta DESC",
              10: "ORDER BY i_delta DESC",
              11: "ORDER BY u_delta DESC",
              12: "ORDER BY d_delta DESC"
            }[int(order)]

    sql = """
        with
        q_min_max_timestamps AS (
              SELECT
                MIN(tsd_timestamp) AS min_date,
                MAX(tsd_timestamp) AS max_date
              FROM monitor_data.table_size_data
              WHERE tsd_host_id = %s
              AND tsd_timestamp >= %s::timestamp
              AND tsd_timestamp <= %s::timestamp
        ),
        q_min_sizes AS (
              SELECT
                tsd_table_id,
                tsd_table_size as min_table_size,
                tsd_index_size as min_index_size,
                tsd_tup_ins + tsd_tup_upd + tsd_tup_del as min_iud,
                tsd_seq_scans + tsd_index_scans as min_s,
                tsd_tup_ins as min_i,
                tsd_tup_upd as min_u,
                tsd_tup_del as min_d
              FROM
                monitor_data.table_size_data st
                JOIN q_min_max_timestamps on true
              WHERE
                st.tsd_host_id = %s
                AND st.tsd_timestamp = q_min_max_timestamps.min_date
        ),
        q_max_sizes AS (
              SELECT
                tsd_table_id,
                tsd_table_size as max_table_size,
                tsd_index_size as max_index_size,
                tsd_tup_ins + tsd_tup_upd + tsd_tup_del as max_iud,
                tsd_seq_scans + tsd_index_scans as max_s,
                tsd_tup_ins as max_i,
                tsd_tup_upd as max_u,
                tsd_tup_del as max_d
              FROM
                monitor_data.table_size_data st
                JOIN q_min_max_timestamps on true
              WHERE
                st.tsd_host_id = %s
                AND st.tsd_timestamp = q_min_max_timestamps.max_date
        )
        SELECT
        *
        FROM (
        SELECT
          t_schema AS schema,
          t_name AS name,
          q_max_sizes.max_table_size AS table_size,
          COALESCE(q_min_sizes.min_table_size, 0) AS min_table_size,
          q_max_sizes.max_table_size - COALESCE(q_min_sizes.min_table_size, 0) AS table_size_delta,
          q_max_sizes.max_index_size AS index_size,
          COALESCE(q_min_sizes.min_index_size, 0) AS min_index_size,
          q_max_sizes.max_index_size - COALESCE(q_min_sizes.min_index_size, 0) AS index_size_delta,
          q_max_sizes.max_iud - COALESCE(q_min_sizes.min_iud, 0) AS iud_delta,
          q_max_sizes.max_s - COALESCE(q_min_sizes.min_s, 0) AS s_delta,
          q_max_sizes.max_i - COALESCE(q_min_sizes.min_i, 0) AS i_delta,
          q_max_sizes.max_u - COALESCE(q_min_sizes.min_u, 0) AS u_delta,
          q_max_sizes.max_d - COALESCE(q_min_sizes.min_d, 0) AS d_delta
        FROM
          q_max_sizes
          LEFT JOIN
          q_min_sizes ON q_min_sizes.tsd_table_id = q_max_sizes.tsd_table_id
          JOIN
          monitor_data.tables ON t_id = q_max_sizes.tsd_table_id
        WHERE
          t_host_id = %s
        ) t
        """ + order_by_sql + limit_sql

    list = datadb.execute(sql, (hostId, date_from, date_to, hostId, hostId, hostId))
    for d in list:

        d['table_size_pretty'] = makePrettySize( d['table_size'] )
        d['index_size_pretty'] = makePrettySize( d['index_size'] )
        d['table_size_delta'] = makePrettySize( d['table_size_delta'] )
        d['index_size_delta'] = makePrettySize( d['index_size_delta'] )
        if d['min_table_size'] > 0:
            d['growth'] = round( ( ( float(d['table_size']) / d['min_table_size'] ) - 1) * 100 , 1 )
        else:
            d['growth'] = 0

        if d['min_index_size'] > 0:
            d['growth_index'] = round( ( ( float(d['index_size']) / d['min_index_size']) - 1) * 100 , 1 )
        else:
            d['growth_index'] = 0
        d['iud_delta'] =  makePrettyCounter(d['iud_delta'])
        d['s_delta'] =  makePrettyCounter(d['s_delta'])
        d['i_delta'] =  makePrettyCounter(d['i_delta'])
        d['u_delta'] =  makePrettyCounter(d['u_delta'])
        d['d_delta'] =  makePrettyCounter(d['d_delta'])

    return list

def fillGraph(graph,data):
    graph.addSeries('Deletes', 'del','#FF0000', None, 2)
    graph.addSeries('Updates', 'upd','#FFFF00', None, 2)
    graph.addSeries('Inserts', 'ins','#885500', None, 2)
    graph.addSeries('Size', 'g')

    for p in data['size']:
        graph.addPoint('g', int(time.mktime(p[0].timetuple()) * 1000) , p[1] )

    if len(data['ins'])>0:
        last_time = int(time.mktime(data['ins'][0][0].timetuple()) * 1000)
        for p in data['ins']:
            current_time = int(time.mktime(p[0].timetuple()) * 1000)
            if current_time > last_time:
                graph.addPoint('ins',current_time , p[1]*1000 / ( current_time - last_time ) )
            last_time = current_time

        last_time = int(time.mktime(data['del'][0][0].timetuple()) * 1000)
        for p in data['del']:
            current_time = int(time.mktime(p[0].timetuple()) * 1000)
            if current_time > last_time:
                graph.addPoint('del',current_time , p[1]*1000 / ( current_time - last_time ) )
            last_time = current_time

        last_time = int(time.mktime(data['upd'][0][0].timetuple()) * 1000)
        for p in data['upd']:
            current_time = int(time.mktime(p[0].timetuple()) * 1000)
            if current_time > last_time:
                graph.addPoint('upd',current_time , p[1]*1000 / ( current_time - last_time ) )
            last_time = current_time
