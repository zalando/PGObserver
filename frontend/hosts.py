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
        return groups

    groups = getGroupsData()
    return groups


def uiShortnameToHostId(shortname):
    for host_id, settings in getHosts().iteritems():
        if settings['uishortname'].lower().replace('-','') == shortname:    # TODO replacing thing is stupid
            return str(host_id)
    return None


def hostIdToUiShortname(hostId):
    return getHosts()[int(hostId)]['uishortname'].lower().replace('-','')


def getHostData():
    conn = DataDB.getDataConnection()
    hosts = {}

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor);

    cur.execute("SELECT * FROM monitor_data.hosts WHERE host_enabled = true ORDER BY host_id ASC;")
    for r in cur:
        rr = dict(r)
        rr['settings'] = json.loads(rr['host_settings'])
        rr['uishortname'] = rr['settings']['uiShortName']
        rr['uilongname'] = rr['settings']['uiLongName']
        hosts[rr['host_id']] = rr

    cur.close()
    conn.close()
    return hosts

def getGroupsData():
    conn = DataDB.getDataConnection()
    groups = {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT * FROM monitor_data.host_groups;")
    for g in cur:
        groups [ g['group_id'] ] = g['group_name']

    cur.close()
    conn.close()
    return groups

