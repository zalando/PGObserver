import psycopg2
import psycopg2.extras
import psycopg2.pool
import contextlib

connection_string = "dbname=pgobserver host=localhost user=postgres password=postgres connect_timeout='3'"
connection_pool = None


@contextlib.contextmanager
def get_conn(pool):
    conn = None
    try:
        conn = pool.getconn()
        conn.autocommit = True
        yield conn
    finally:
        if conn:
            pool.putconn(conn)


def get_cursor(conn, cursor_factory=None):
    cur = None
    if cursor_factory:
        cur = conn.cursor(cursor_factory=cursor_factory)
    else:
        cur = conn.cursor()
    cur.execute("SET work_mem TO '256MB';")
    return cur


def set_connection_string_and_pool_size(conn_string, pool_size=5):
    global connection_string
    connection_string = conn_string
    global connection_pool
    connection_pool = psycopg2.pool.ThreadedConnectionPool(pool_size, pool_size, conn_string)


def mogrify(sql, params=None):
    with get_conn(connection_pool) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return cur.mogrify(sql, params)


def executeAsDict(sql, params=None):
    with get_conn(connection_pool) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur.fetchall(), [x.name for x in cur.description]


def execute(sql, params=None):
    with get_conn(connection_pool) as conn:
        cur = get_cursor(conn)
        cur.execute(sql, params)
        return cur.fetchall(), [x.name for x in cur.description]


if __name__ == '__main__':
    set_connection_string_and_pool_size(connection_string)
    print execute('select 1 as a')
    print executeAsDict('select 1 as a')
