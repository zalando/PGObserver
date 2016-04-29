import contextlib
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2.extensions import adapt


connection_pool = None
connection_string = "dbname=local_pgobserver_db host=localhost user=postgres password=postgres connect_timeout='3'"


def init_connection_pool(url=connection_string, max_conns=10):
    global connection_pool
    if connection_pool and not connection_pool.closed():
        connection_pool.closeall()
    connection_pool = psycopg2.pool.ThreadedConnectionPool(1, max_conns, url)


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


def setConnectionString(conn_string):
    global connection_string
    connection_string = conn_string
    init_connection_pool(conn_string)


def setConnectionString(host, port, dbname, username, password, connect_timeout=10):
    global connection_string
    connection_string = 'host={} port={} dbname={} user={} password={} connect_timeout={}'.format(host, port, dbname,
                                                                                                  username, password,
                                                                                                  connect_timeout)
    init_connection_pool(connection_string)


def getDataConnection(autocommit=True):
    conn = psycopg2.connect(connection_string)
    if autocommit:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SET statement_timeout TO '60s';")
    cur.close()
    return conn


def execute(sql, params=None):
    result = []
    try:
        with get_conn(connection_pool) as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SET statement_timeout TO '60s'")
            cur.execute(sql, params)
            if cur.statusmessage.startswith('SELECT') or cur.description:
                result = cur.fetchall()
            else:
                result = [{'rows_affected': str(cur.rowcount)}]
    except:
        logging.exception('failed to execute "{}" on datastore'.format(sql))
    return result


def isDataStoreConnectionOK():
    data = []
    try:
        data = execute('select 1 as x')
    except Exception as e:
        logging.error(e)
    if data:
        return data[0]['x'] == 1
    else:
        return False


def executeOnConnection(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    if cur.statusmessage.startswith('SELECT') or cur.description:
        result = cur.fetchall()
    else:
        result = [{'rows_affected': str(cur.rowcount)}]
    return result


def executeOnHost(hostname, port, dbname, user, password, sql, params=None):
    logging.debug('executeOnHost(%s,%s,%s,%s,%s,%s):', hostname, port, dbname, user, sql, params)
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


def mogrifyOnHost(hostname, port, dbname, user, password, sql, params=None):
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
        conn = getDataConnection()
        cur = conn.cursor()
        cur.copy_from(file=file_obj, table=table_name, columns=columns)
        return cur.rowcount
    finally:
        if conn and not conn.closed:
            conn.close()



if __name__ == '__main__':
    init_connection_pool(connection_string)
    print(execute('select 1 as x'))
    # print(executeOnHost('localhost', 5432, 'local_pgobserver_db', 'postgres', 'postgres', 'select 1'))
