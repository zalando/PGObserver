import psycopg2
import psycopg2.extras


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

def execute(sql, params=None):
    conn = None
    try:
        conn = getDataConnection()
        cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur.fetchall()
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
    except Exception, e:
        print ('ERROR execution failed on {}: {}'.format(hostname, e))
        msg = 'ERROR execution failed on {}: {}'.format(hostname, e)
    finally:
        if conn and not conn.closed:
            conn.close()
    return msg, data


if __name__ == '__main__':
    print executeOnHost('localhost', 5432, 'local_pgobserver_db', 'postgres', 'postgres', 'select 1')
