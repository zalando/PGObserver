from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import datadb
from psycopg2.extensions import adapt


def getIndexesDataForTable(host, full_name, date_from, date_to):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(getSingleTableSql(host, full_name, date_from, date_to))
    data = cur.fetchall()
    cur.close()
    datadb.closeDataConnection(conn)

    all_data = []
    d = { 'size' : [], 'scan' : [], 'tup_read': [] } #, 'tup_fetch' : [] }

    last_scan = None
    last_tup_read = None
#    last_tup_fetch = None
    last_name = None
    last_index_size = 0
    last_total_end_size = 0
    last_pct_of_total_end_size = 0

    for r in data:
        if last_name != None:
            if last_name != r['name'] and len(d['size']) > 0:
                all_data.append({'index_name':last_name, 'data':d, 'last_index_size': round(last_index_size / 1024**2), 'total_end_size': round(last_total_end_size / 1024**2), 'pct_of_total_end_size':last_pct_of_total_end_size})
                d = { 'size' : [], 'scan' : [], 'tup_read': [] } # , 'tup_fetch' : [] }
            
            d['size'].append( ( r['timestamp'] , r['size'] ) )
            d['scan'].append( ( r['timestamp'] , 0 if last_scan > r['scan'] else r['scan'] - last_scan ) )
            d['tup_read'].append( ( r['timestamp'] , 0 if last_tup_read > r['tup_read'] else r['tup_read'] - last_tup_read ) )
#            d['tup_fetch'].append( ( r['timestamp'] , 0 if last_tup_fetch > r['tup_fetch'] else r['tup_fetch'] - last_tup_fetch ) )


        last_scan = r['scan']
        last_tup_read = r['tup_read']
#        last_tup_fetch = r['tup_fetch']
        last_name = r['name']
        last_index_size = r['size']
        last_total_end_size = r['total_end_size']
        last_pct_of_total_end_size = r['pct_of_total_end_size']

    if len(data) > 0:
        all_data.append({'index_name':last_name, 'data':d, 'last_index_size': round(last_index_size / 1024**2), 'total_end_size': round(last_total_end_size / 1024**2), 'pct_of_total_end_size':last_pct_of_total_end_size})

    return all_data


def getSingleTableSql(host, full_table_name, date_from, date_to=None):

    interval = "AND iud_timestamp BETWEEN %s::timestamp and %s::timestamp" % (adapt(date_from),adapt(date_to), )
    if date_to==None:
        interval = " AND iud_timestamp > %s::timestamp" % (adapt(date_from), )
    
    schema = full_table_name.split('.')[0]
    table = full_table_name.split('.')[1]
    sql = """
WITH q_end_size  AS (
    SELECT iud_timestamp as end_timestamp,
           sum(iud_size) as total_end_size
      FROM monitor_data.index_usage_data,
           monitor_data.indexes
     WHERE i_host_id = """ + str(adapt(host)) + """
       AND i_schema = """ + str(adapt(schema)) + """
       AND iud_host_id = """ + str(adapt(host)) + """
       AND i_table_name = """ + str(adapt(table)) + """
       AND iud_index_id = i_id
       """+interval+"""
     GROUP
        BY iud_timestamp
     ORDER
        BY iud_timestamp DESC
      LIMIT 1
)
SELECT iud_timestamp as timestamp,
     i_name as name,
     iud_scan as scan,
     iud_tup_read as tup_read,
     --iud_tup_fetch as tup_fetch,
     iud_size as size,
     total_end_size,
     round (iud_size / total_end_size*100::numeric, 1) as pct_of_total_end_size
FROM monitor_data.index_usage_data,
     monitor_data.indexes,
     q_end_size
WHERE i_host_id = """ + str(adapt(host)) + """
 AND i_schema = """ + str(adapt(schema)) + """
 AND iud_host_id = """ + str(adapt(host)) + """
 AND i_table_name = """ + str(adapt(table)) + """
 AND iud_index_id = i_id
 """+interval+"""
ORDER BY i_name, iud_timestamp ASC
    """

    return sql
