'''
Created on Sep 28, 2011

@author: jmussler
'''
import DataDB
import psycopg2
import psycopg2.extras
import json

hosts = None
groups = None

def getHosts():
    global hosts
    if hosts != None:
        return hosts

    hosts = getHostData()
    return hosts



def getGroups():
    global groups
    if groups != None:
        return groups;

    groups = getGroupsData()
    return groups


#def getHosts():
#    return { 1 : 'bm-master', 2: 'customerindex' , 3: 'customer1', 4: 'shop-master' , 5: 'addr-master', 6: 'zalos' , 7 : 'otrs', 8 : 'partner' , 9 : 'export' }

def getHostData():
    conn = DataDB.getDataConnection()
    hosts = {}

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor);

    cur.execute("SELECT * FROM monitor_data.hosts WHERE host_enabled = true ORDER BY host_id ASC;")
    for r in cur:
        rr = dict(r);
        rr['settings'] = json.loads(rr['host_settings'])
        hosts[rr['host_id']] = rr;

    cur.close();
    conn.close();
    return hosts;

def getGroupsData():
    conn = DataDB.getDataConnection()
    groups = {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor);

    cur.execute("SELECT * FROM monitor_data.host_groups;")
    for g in cur:
        groups [ g['group_id'] ] = g['group_name']

    cur.close();
    conn.close();
    return groups;

