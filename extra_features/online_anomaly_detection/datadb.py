import contextlib
import json
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2.extensions import adapt


connection_pool = None
connection_string = "dbname=local_pgobserver_db host=localhost user=postgres password=postgres connect_timeout='3'"


def init_connection_pool(max_conns=1, **kwargs):
    global connection_pool
    if connection_pool and not connection_pool.closed():
        connection_pool.closeall()
    connection_pool = psycopg2.pool.ThreadedConnectionPool(1, max_conns, **kwargs)


@contextlib.contextmanager
def get_conn(pool, autocommit=True):
    conn = None
    try:
        conn = pool.getconn()
        conn.autocommit = autocommit
        yield conn
    finally:
        if conn:
            pool.putconn(conn)


def set_connection_string(conn_string):
    global connection_string
    connection_string = conn_string
    init_connection_pool(conn_string)


def set_connection_string(host, port, dbname, username, connect_timeout=10):
    global connection_string
    connection_string = 'host={} port={} dbname={} user={} connect_timeout={}'.format(host, port, dbname, username, connect_timeout)
    init_connection_pool(host=host, port=port, dbname=dbname, username=username, connect_timeout=connect_timeout)


def get_data_connection(autocommit=True):
    conn = psycopg2.connect(connection_string)
    if autocommit:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SET statement_timeout TO '60s';")
    cur.close()
    return conn


def execute(sql, params=None):
    result = None
    with get_conn(connection_pool) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET statement_timeout TO '600s'")
        cur.execute(sql, params)
        if cur.statusmessage.startswith('SELECT') or cur.description:
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
    return result


def mogrify(sql, params=None):
    with get_conn(connection_pool) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return cur.mogrify(sql, params)


def is_data_store_connection_ok():
    try:
        data = execute('select 1 as x')
    except Exception as e:
        logging.error(e)
        return False
    return data[0]['x'] == 1


def execute_on_connection(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    if cur.statusmessage.startswith('SELECT') or cur.description:
        result = cur.fetchall()
    else:
        result = [{'rows_affected': str(cur.rowcount)}]
    return result


def execute_on_host(hostname, port, dbname, user, password, sql, params=None):
    conn = None
    result = []

    try:
        conn = psycopg2.connect(host=hostname, port=port, dbname=dbname, user=user, password=password, connect_timeout='5')
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET statement_timeout TO '120s';")
        cur.execute(sql, params)
        if cur.statusmessage.startswith('SELECT') or cur.description:
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
    finally:
        if conn and not conn.closed:
            conn.close()
    return result


def mogrify_on_host(hostname, port, dbname, user, password, sql, params=None):
    conn = None

    try:
        conn = psycopg2.connect(host=hostname, port=port, dbname=dbname, user=user, password=password, connect_timeout='5')
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return cur.mogrify(sql, params)
    finally:
        if conn and not conn.closed:
            conn.close()


def copy_from(file_obj, table_name, columns):
    conn = None
    try:
        conn = get_data_connection()
        cur = conn.cursor()
        cur.copy_from(file=file_obj, table=table_name, columns=columns)
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == '__main__':
    # init_connection_pool(connection_string)
    init_connection_pool(host='localhost', dbname='local_pgobserver_db', user='postgres', password='postgres')
    print(execute('select 1 as x'))
    print(execute_on_host('localhost', 5432, 'local_pgobserver_db', 'postgres', 'postgres', 'select 1'))
