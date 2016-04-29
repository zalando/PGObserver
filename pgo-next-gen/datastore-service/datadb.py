import logging
import psycopg2
import psycopg2.extras
from io import StringIO
from psycopg2.extensions import adapt

connection_string = "dbname=local_pgobserver_db host=localhost user=postgres password=postgres connect_timeout='3'"

def setConnectionString(conn_string):
    global connection_string
    connection_string = conn_string

def getDataConnection():
    conn = psycopg2.connect(connection_string)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SET work_mem TO '256MB';")
    cur.close()
    return conn

def closeDataConnection(c):
    c.close()

def mogrify(sql, params=None):
    conn = None
    result = None
    try:
        conn = getDataConnection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return cur.mogrify(sql, params)
    finally:
        if conn and not conn.closed:
            conn.close()

def execute(sql, params=None):
    conn = None
    result = None
    try:
        conn = getDataConnection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        if cur.statusmessage.startswith('SELECT') or cur.description:
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
        return result
    finally:
        if conn and not conn.closed:
            conn.close()


def executeOnHost(hostname, port, dbname, user, password, sql, params=None):
    data = []
    msg = None
    conn = None
    try:
        conn = psycopg2.connect(host=hostname, port=port, dbname=dbname, user=user, password=password, connect_timeout='3')
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        data = cur.fetchall()
    except Exception as e:
        logging.error(e)
        msg = 'ERROR execution failed on {}: {}'.format(hostname, e)
    finally:
        if conn and not conn.closed:
            conn.close()
    return msg, data


def copy_from(file_obj, table_name, columns):
    conn = None
    try:
        conn = getDataConnection()
        cur = conn.cursor()
        cur.copy_from(file=file_obj, table=table_name, columns=columns)
    finally:
        if conn and not conn.closed:
            conn.close()


def dataset_to_delimited_stringio(data, delimiter='\t'):
    data_as_string = StringIO()

    for row in data:
        # row_as_str = [str(adapt(x)) for x in row]
        row_as_str = [str(x) for x in row]      # is escaping needed?
        csv_row = delimiter.join(row_as_str)
        data_as_string.write(csv_row)
        data_as_string.write('\n')

    data_as_string.seek(0)
    return data_as_string


if __name__ == '__main__':
    print(executeOnHost('localhost', 5432, 'local_pgobserver_db', 'postgres', 'postgres', 'select 1'))

    from io import StringIO # TODO testfile
    sio = StringIO()
    sio.write('a\t1\n')
    sio.write('a\t2\n')
    sio.seek(0)
    execute('create table copy_test(c1 text, c2 int)')
    copy_from(sio, 'copy_test', ('c1', 'c2'))
    print(execute('select * from copy_test'))
