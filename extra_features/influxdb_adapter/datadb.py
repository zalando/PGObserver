import psycopg2
import psycopg2.extras
import psycopg2.pool
import contextlib


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


def init_connection_pool(pool_size=5, **libpq_params):
    global connection_pool
    if 'password' in libpq_params and not libpq_params['password']:
        libpq_params.pop('password')
    connection_pool = psycopg2.pool.ThreadedConnectionPool(1, pool_size, **libpq_params)


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
    init_connection_pool(host='localhost', user='postgres', password='postgres')
    print execute('select current_database() as a')
    print executeAsDict('select 1 as a')
