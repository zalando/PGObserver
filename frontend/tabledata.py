'''
Created on 19.09.2011

@author: Jan
'''
from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import DataDB
from psycopg2.extensions import adapt

def getSizeTrendSQL(host_id = None):
    if host_id == None:
        host_sql = ''
    else:
        host_sql = ' AND t_host_id = %s' % (adapt(host_id),)

    sql = """SELECT t_host_id,
                     tsd_timestamp,
                    ( SUM(tsd_table_size)+SUM(tsd_index_size) ) AS size,
                    SUM(tsd_tup_ins) AS s_ins,
                    SUM(tsd_tup_upd) AS s_upd,
                    SUM(tsd_tup_del) AS s_del
                FROM monitor_data.table_size_data
                JOIN monitor_data.tables ON t_id = tsd_table_id
                WHERE tsd_timestamp > 'now'::timestamp - '9 days'::interval""" + host_sql + """
              GROUP BY t_host_id, tsd_timestamp ORDER BY t_host_id, tsd_timestamp"""

    return sql

def getIOTrendSQL(host_id = None):
    if host_id == None:
        host_sql = ''
    else:
        host_sql = ' AND t_host_id = %s' % (adapt(host_id),)

# group by timestamp is ok, gatherer puts same timestamp for every interval into all values

    sql = """SELECT t_host_id, tio_timestamp, SUM(tio_heap_read), SUM(tio_idx_read)
               FROM monitor_data.table_io_data, monitor_data.tables
              WHERE tio_table_id = t_id
                AND tio_timestamp > 'now'::timestamp - '9 days'::interval
                """ + host_sql + """
            GROUP BY t_host_id, tio_timestamp ORDER BY t_host_id , tio_timestamp
                """

    return sql


def getDatabaseSizes(host_id = None):
    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(getSizeTrendSQL(host_id))
    size_data = {}

    current_host = 0

    last_timestamp = None

    for record in cur:

        if record['t_host_id'] != current_host:
            current_host = record['t_host_id']
            set_ins = False
            set_del = False
            set_upd = False

            l_ins = None
            l_upd = None
            l_del = None
            last_timestamp = None

        if last_timestamp == None:
            last_timestamp = int(time.mktime(record['tsd_timestamp'].timetuple()) * 1000)

        if not record['t_host_id'] in size_data:
            size_data[record['t_host_id']] = { 'size' : [] , 'ins': [], 'upd': [], 'del':[] }

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

        size_data[record['t_host_id']]['size'].append( ( record['tsd_timestamp'] , record['size'] ) )
        size_data[record['t_host_id']]['ins'].append( ( record['tsd_timestamp'] , max( record['s_ins'] - l_ins , 0)  ) )
        size_data[record['t_host_id']]['del'].append( ( record['tsd_timestamp'] , max( record['s_del'] - l_del , 0)  ) )
        size_data[record['t_host_id']]['upd'].append( ( record['tsd_timestamp'] , max( record['s_upd'] - l_upd , 0)  ) )

        l_ins = record['s_ins']
        l_upd = record['s_upd']
        l_del = record['s_del']

        last_timestamp = int(time.mktime(record['tsd_timestamp'].timetuple()) * 1000)

    cur.close()
    DataDB.closeDataConnection(conn)

    return size_data

def makePrettySize(size):
    mb = int( size / 1000000 )
    return str(mb) + " MB"

def getSingleTableSql(host, name, interval=None):

    if interval==None:
        interval = "AND tsd_timestamp > ('now'::timestamp - '14 days'::interval)"
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
     WHERE tsd_table_id = ( SELECT t_id FROM monitor_data.tables WHERE t_schema || '.' || t_name = '""" + str(adapt(name)) + """' AND t_host_id = """ + str(adapt(host)) + """ )
       """+interval+"""
      ORDER BY tsd_timestamp ASC
    """

    return sql

def getSingleTableIOSql(host, name, interval=None):

    if interval==None:
        interval = "AND tio_timestamp > ('now'::timestamp - '14 days'::interval)"
    else:
        if 'interval' in interval:
            interval = "AND tio_timestamp > %s::interval" % ( adapt(interval['interval']), )
        else:
            interval = "AND tio_timestamp BETWEEN %s::timestamp and %s::timestamp" % (adapt(interval['from']),adapt(interval['to']), )

    sql = """
    SELECT tio_table_id, tio_timestamp, tio_heap_read, tio_heap_hit, tio_idx_read,
           tio_idx_hit
      FROM monitor_data.table_io_data
     WHERE tio_table_id = ( SELECT t_id FROM monitor_data.tables WHERE t_schema || '.' || t_name = '""" + str(adapt(name)) + """' AND t_host_id = """ + str(adapt(host)) + """ )
       """+interval+"""
      ORDER BY tio_timestamp ASC
    """

    return sql

def getTableIOData(host, name, interval = None):
    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(getSingleTableIOSql(host,name,interval))

    d = { 'heap_read' : [], 'heap_hit' : [], 'index_read' : [], 'index_hit': [] }

    last_hr = None
    last_hh = None
    last_ir = None
    last_ih = None
    last_timestamp = 0

    for r in cur:

        if int(time.mktime(r['tio_timestamp'].timetuple()) * 1000) - last_timestamp <= ( 15*60*1000 ):
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

        last_timestamp = int(time.mktime(r['tio_timestamp'].timetuple()) * 1000)


    cur.close()
    DataDB.closeDataConnection(conn)

    return d


def getTableData(host, name, interval = None):
    conn = DataDB.getDataConnection()
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

        if int(time.mktime(r['tsd_timestamp'].timetuple()) * 1000) - last_timestamp <= ( 15*60*1000 ):
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
    DataDB.closeDataConnection(conn)

    return d


def getTopTables(hostId=1, limit=10, order=None):
    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if limit == None:
        limit = ""
    else:
        limit = """ LIMIT """ + str(adapt(limit))

    if order == None:
        order = 2

    order = { 1: "ORDER BY schema ASC,name ASC ",
              2: "ORDER BY table_size DESC" ,
              3: "ORDER BY table_size - min_table_size DESC",
              4: "ORDER BY CASE WHEN min_table_size > 0 THEN table_size::float / min_table_size ELSE 0 END DESC",
              5: "ORDER BY index_size DESC",
              6: "ORDER BY index_size - min_index_size DESC",
              7: "ORDER BY CASE WHEN min_index_size > 0 THEN index_size::float / min_index_size ELSE 0 END DESC" }[int(order)]

    cur.execute("""SELECT MAX(tsd_timestamp) AS max_date
                     FROM monitor_data.table_size_data
                    WHERE tsd_table_id = ( SELECT t_id FROM monitor_data.tables WHERE t_host_id = """+str(adapt(hostId))+""" LIMIT 1)""")

    maxTime = None
    for record in cur:
        maxTime = record['max_date']

    if maxTime == None:
        return []

    sql = """SELECT * FROM ( SELECT t_schema AS schema,
                          t_name AS name,
                          tsd_table_size AS table_size,
                          tsd_index_size AS index_size,
                          COALESCE ( ( SELECT MIN(tsd_table_size) FROM monitor_data.table_size_data st WHERE td.tsd_table_id = st.tsd_table_id AND st.tsd_timestamp > ('now'::timestamp - '7 days'::interval) ), 0) AS min_table_size,
                          COALESCE ( ( SELECT MIN(tsd_index_size) FROM monitor_data.table_size_data st WHERE td.tsd_table_id = st.tsd_table_id AND st.tsd_timestamp > ('now'::timestamp - '7 days'::interval) ), 0) AS min_index_size

                     FROM monitor_data.table_size_data td
                     JOIN monitor_data.tables ON t_id = td.tsd_table_id
                    WHERE td.tsd_timestamp = """ + adapt(maxTime) + """ AND t_host_id = """ + str(adapt(hostId)) + """ ) _t """ + order + """ """ + limit

    cur.execute( sql )

    list = []
    for r in cur:

        d = {}
        for k in r.keys():
            d[k] = r[k]

        d['table_size_pretty'] = makePrettySize( r['table_size'] )
        d['index_size_pretty'] = makePrettySize( r['index_size'] )
        d['table_size_delta'] = makePrettySize( r['table_size'] - r['min_table_size'] )
        d['index_size_delta'] = makePrettySize( r['index_size'] - r['min_index_size'] )
        if r['min_table_size'] > 0:
            d['growth'] = round( ( ( float(r['table_size']) / r['min_table_size'] ) - 1) * 100 , 1 )
        else:
            d['growth'] = 0

        if r['min_index_size'] > 0:
            d['growth_index'] = round( ( ( float(r['index_size']) / r['min_index_size']) - 1) * 100 , 1 )
        else:
            d['growth_index'] = 0

        list.append(d)

    return list

def fillGraph(graph,data):
    graph.addSeries('Deletes', 'del','#FF0000', None, 2)
    graph.addSeries('Updates', 'upd','#FF8800', None, 2)
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
