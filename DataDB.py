'''
Created on Sep 19, 2011

@author: jmussler
'''
import psycopg2
import psycopg2.extras

connection_string = "dbname=dbmonitor host=localhost user=postgres password=postgres"

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

def execute(sql, params=None):
    conn = None
    try:
        conn = getDataConnection()
        cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        if conn and not conn.closed:
            conn.close
        
