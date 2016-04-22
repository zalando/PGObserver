import datadb
import psycopg2
import psycopg2.extras
import json

hosts = None
all_hosts = None
groups = None


def getHosts(force_refresh_from_db=False):
    global hosts
    if not force_refresh_from_db and hosts:
        return hosts

    hosts = getHostData()
    return hosts


def getGroups():
    global groups
    if groups != None:
        return groups

    groups = getGroupsData()
    return groups


def resetHostsAndGroups():
    global hosts
    global groups
    hosts = None
    groups = None


def uiShortnameToHostId(shortname):
    for host_id, settings in getAllHosts().iteritems():
        if settings['uishortname'].lower().replace('-','') == shortname:    # TODO replacing thing is stupid
            return host_id
    raise Exception('specified uiShortName {0} not found! check the monitor_data.hosts table...'.format(shortname,))


def hostIdToUiShortname(hostId):
    return getAllHosts()[int(hostId)]['uishortname'].lower().replace('-','')

def getAllHostNames():
    ret = [ x[1]['host_name'] for x in getHosts().items() ]
    return sorted(ret)

def uiShortNameToHostName(ui_short):
    for id, data in getAllHosts().iteritems():
        if data['uishortname'] == ui_short:
            return data['host_name']
    raise Exception('No host_name match found for ' + ui_short)


def ensureHostIdAndUIShortname(hostIdOrHostUIShortname):
    if str(hostIdOrHostUIShortname).isdigit():
        hostId = int(hostIdOrHostUIShortname)
        hostUiName = hostIdToUiShortname(hostId)
    else:
        hostUiName = hostIdOrHostUIShortname
        hostId = int(uiShortnameToHostId(hostUiName))
    return hostId, hostUiName

def getHostData():
    conn = datadb.getDataConnection()
    hosts = {}

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT host_id, host_name, host_port, host_db, host_settings, host_group_id, host_enabled,"
                " host_ui_shortname, host_ui_longname"
                " FROM monitor_data.hosts WHERE host_enabled = true ORDER BY host_id ASC;")
    for r in cur:
        rr = dict(r)
        rr['settings'] = json.loads(rr['host_settings'])
        rr['uishortname'] = r['host_ui_shortname'].lower().replace('-','')
        rr['uilongname'] = r['host_ui_longname']
        hosts[rr['host_id']] = rr

    cur.close()
    conn.close()
    return hosts

def getAllHostsData():
    conn = datadb.getDataConnection()
    hosts = {}

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT host_id, host_name, host_port, host_db, host_settings, host_group_id, host_enabled,"
                " host_ui_shortname, host_ui_longname"
                " FROM monitor_data.hosts ORDER BY host_id ASC;")
    for r in cur:
        r['uishortname'] = r['host_ui_shortname'].lower().replace('-','')
        r['uilongname'] = r['host_ui_longname']
        hosts[r['host_id']] = r

    cur.close()
    conn.close()
    return hosts

def getAllHosts(force_refresh_from_db=False):
    global all_hosts
    if not force_refresh_from_db and all_hosts:
        return all_hosts

    all_hosts = getAllHostsData()
    return all_hosts

def getAllHostUinamesSorted():
    return sorted([x[1]['uishortname'] for x in getAllHosts().items()])

def getLastInsertedHostUserAndPassword():
    sql_user = """select host_user from hosts where host_enabled and host_user is not null order by host_created desc limit 1"""
    sql_pass = """select host_password from hosts where host_enabled and host_password is not null order by host_created desc limit 1"""
    return (sql_user, sql_pass)


def saveHost(hostDict):
    sql_cols = ""
    sql_vals = []
    if hostDict['host_id'] == '':
       hostDict.pop('host_id')


    if 'host_id' not in hostDict:   # insert new host
        sql_last_user, sql_last_pw = getLastInsertedHostUserAndPassword()
        sql = "INSERT INTO hosts ("
        for k, v in hostDict.iteritems():
            sql_cols += k + ","
            sql_vals.append(v)
        sql_cols += "host_user, host_password"
        sql += sql_cols + ") VALUES (" + ("%s," * len(sql_vals))
        sql += "COALESCE((" + sql_last_user + "),'dummyuser'), COALESCE((" + sql_last_pw + "), 'dummypass')) RETURNING host_id"
        ret = datadb.execute(sql, tuple(sql_vals))
        return ret[0]['host_id']
    else:   # update
        host_id = hostDict['host_id']
        hostDict.pop('host_id')
        hostDict.update({'host_last_modified':'now'})
        sql = "UPDATE hosts SET\n"
        for k, v in hostDict.iteritems():
            sql_cols += k + "=%s,\n"
            sql_vals.append(v)
        sql_cols = sql_cols.strip(",\n")
        sql += sql_cols
        sql += "\nWHERE host_id = " + host_id + " RETURNING host_id"
        ret = datadb.execute(sql, tuple(sql_vals))
        return ret[0]['host_id']


def getGroupsData():
    conn = datadb.getDataConnection()
    groups = {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT * FROM monitor_data.host_groups;")
    for g in cur:
        groups [ g['group_id'] ] = g['group_name']

    cur.close()
    conn.close()
    return groups

def getHostByHostname(hostname):
    for h_id, h_data in getHosts().iteritems():
        if h_data['host_name'] == hostname:
            return h_data
    raise Exception('Could not find host: ' +hostname)

def getHostUIShortnameByHostname(hostname):
    return getHostByHostname(hostname)['uishortname']

def getHostnameByHostId(hostId):
    return getHosts()[hostId]['host_name']

def getHostsDataForConnecting(hostname='all'):
    q_active_hosts="""
        select
            host_id,
            host_name,
            host_port,
            host_user,
            host_password,
            host_db
        from monitor_data.hosts
        where host_enabled
        and (%s = 'all' or host_name=%s)
        """
    return datadb.execute(q_active_hosts, (hostname, hostname))

def getHostsDataForConnectingByUIShortname(shortname='all'):
    q_active_hosts="""
        select
            host_id,
            host_name,
            host_port,
            host_user,
            host_password,
            host_db
        from monitor_data.hosts
        where host_enabled
        and (%s = 'all' or host_ui_shortname=%s)
        """
    return datadb.execute(q_active_hosts, (shortname, shortname))


def isHostFeatureEnabled(hostId, featureText):
    hostData = getHosts()[hostId]
    if featureText not in hostData['settings']:
        return False
    return hostData['settings'][featureText] > 0


def getActiveFeatures(hostId):
    hostData = getHosts()[hostId]
    return [s for s, v in hostData['settings'].iteritems() if v > 0]


def getHostsWithFeature(feature):
    ret = {}
    for hostid, data in getHosts().iteritems():
        if data['settings'].get(feature, 0) > 0:
            ret[hostid] = data
    return ret


def getHostsWithFeatureAsShortnames(feature):
    hosts_with_schema_gathering_enabled = getHostsWithFeature(feature)
    uishortnames = [x['host_ui_shortname'] for x in hosts_with_schema_gathering_enabled.values()]
    uishortnames.sort()
    return uishortnames


if __name__ == '__main__':
    # print (getAllHostNames())
    # print (getHostsDataForConnecting())
    # print (isHostFeatureEnabled(3, 'loadGatherInterval'))
    # print (getHostsWithFeature('indexStatsGatherInterval'))
    print (getActiveFeatures(1000))
    print (getAllHostUinamesSorted())
