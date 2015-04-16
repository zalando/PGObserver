import psycopg2
import psycopg2.extras

connection_string = "dbname=pgobserver host=localhost user=postgres password=postgres connect_timeout='3'"


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
    conn = getDataConnection()
    cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cur.mogrify(sql, params)


def executeAsDict(sql, params=None):
    conn = None
    try:
        conn = getDataConnection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur.fetchall(), [x.name for x in cur.description]
    finally:
        if conn and not conn.closed:
            conn.close()


def execute(sql, params=None):
    conn = None
    try:
        conn = getDataConnection()
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall(), [x.name for x in cur.description]
    finally:
        if conn and not conn.closed:
            conn.close()


def executeUsingExistingConn(conn, sql, params=None):
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall(), [x.name for x in cur.description]
    finally:
        if cur and not cur.closed:
            cur.close()


if __name__ == '__main__':
    print execute('select 1')
