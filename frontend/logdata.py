### log data functions
import psycopg2
import psycopg2.extras
import DataDB

_tables = { 1 : 'bm_log', 2 : 'customeridx_log' }

_filter = ''

def setFilter(users):
    global _filter

    _filter = ''
    for u in users:
        if _filter != '':
            _filter += ' OR '
        _filter += "user_name LIKE '"+u+"'"

    if _filter != '':
        _filter = '(' + _filter + ')'

def get_temporary_files_query(host_id,interval = None):
    table_name = _tables[host_id]

    if(interval==None):
        interval = "AND log_time > ( 'now'::timestamp - '15 days'::interval ) "
    else:
        interval = "AND log_time > " + interval

    sql = """SELECT date_trunc('hour'::text, log_time) + floor(date_part('minute'::text, log_time) / 10::double precision) * '00:10:00'::interval AS xaxis,
                     count(1) AS yaxis
                FROM log_file_data."""+table_name+""" WHERE message like '%temporary%' """+interval+""" GROUP BY date_trunc('hour'::text, log_time) + floor(date_part('minute'::text, log_time) / 10::double precision) * '00:10:00'::interval ORDER BY 1 asc"""

    return sql

def load_temporary_lines(host_id, interval = None):

    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(get_temporary_files_query(host_id,interval))
    l = []
    for row in cur:
        l.append ( ( row['xaxis'], row['yaxis'] ) )

    return l

def get_filted_query(host_id, _filter = None, interval = None):
    table_name = _tables[host_id]

    if _filter == None:
        _filter = ""

    if(interval==None):
        interval = ""
    else:
        interval = "log_time > " + interval

    if interval != "" and _filter != "":
        interval = " AND " + interval

    sql = """SELECT date_trunc('hour'::text, log_time) + floor(date_part('minute'::text, log_time) / 10::double precision) * '00:10:00'::interval AS xaxis,
                     count(1) AS yaxis
                FROM log_file_data."""+table_name+""" WHERE """+_filter+""" """+interval+""" GROUP BY date_trunc('hour'::text, log_time) + floor(date_part('minute'::text, log_time) / 10::double precision) * '00:10:00'::interval ORDER BY 1 asc"""

    print sql

    return sql

def load_filter_lines(host_id, _filter = None, interval = None):

    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(get_filted_query(host_id,_filter,interval))
    l = []
    last_x = 0
    for row in cur:
        l.append ( ( row['xaxis'], row['yaxis'] ) )

    return l


def load_wait_lines(host_id, _filter = None, interval = None):
    return load_filter_lines(host_id, _filter + " AND query like '%pg_advisory%' ", interval)

def load_error_lines(host_id, _filter = None, interval = None):
    return load_filter_lines(host_id, _filter + " AND error_severity = 'ERROR' ", interval)

def load_user_error_lines(host_id, _filter = None, interval = None):
    return load_filter_lines(host_id, " NOT " + _filter + " AND error_severity = 'ERROR' ", interval)

def load_timeout_lines(host_id, _filter = None, interval = None):
    return load_filter_lines(host_id, _filter + " AND message like '%canceling statement due to statement timeout%' ", interval)

def load_user_timeout_lines(host_id, _filter = None, interval = None):
    return load_filter_lines(host_id, " NOT " + _filter + " AND message like '%canceling statement due to statement timeout%' ", interval)
