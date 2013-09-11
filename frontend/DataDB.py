'''
Created on Sep 19, 2011

@author: jmussler
'''
import psycopg2
import psycopg2.extras

#connection_string = "dbname=dbmonitor host=localhost user=postgres password=postgres"
connection_string = "dbname=dbmonitor host=localhost user=pgobserver_frontend_test password=ndowifwensa"

def setConnectionString(conn_string):
    global connection_string
    connection_string = conn_string

def getDataConnection():
    conn = psycopg2.connect(connection_string)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SET work_mem TO '64MB';")
    cur.close()
    return conn

def closeDataConnection(c):
    c.close()

def getActiveHosts(hostname='all'):
    q="""
        select host_id, host_name, host_user, host_password,host_db
        from monitor_data.hosts
        where host_enabled
        and (%s = 'all' or host_name=%s)
        --and host_gather_group == 'monitor01'
        """
    conn = getDataConnection()
    cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(q, (hostname,hostname))
    ret = cur.fetchall()
    conn.close()
    return ret