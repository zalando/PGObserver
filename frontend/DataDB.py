'''
Created on Sep 19, 2011

@author: jmussler
'''
import psycopg2
import psycopg2.extras

connection_string = "dbname=dbmonitor host=localhost user= password=postgres"

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
