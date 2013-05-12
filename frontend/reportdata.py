from __future__ import print_function
'''
Created on Feb 1, 2012

@author: jmussler
'''

import DataDB
import psycopg2
import psycopg2.extras
from collections import defaultdict
import hosts

#@funccache.lru_cache(60,25)
def getLoadReportData():
    conn = DataDB.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("""select load_host_id AS id,
                          extract(week from load_timestamp)::text AS kw,
                          round(avg(load_1min_value)/100,2) AS avg,
                          round(max(load_1min_value)/100,2) AS max,
                          to_char(min(load_timestamp::date),'dd.mm.YYYY') AS min_date,
                          to_char(max(load_timestamp::date),'dd.mm.YYYY') AS max_date,
                          min(load_timestamp::date) AS sort_date
                     from monitor_data.host_load , monitor_data.hosts
                    where host_id = load_host_id
                      and host_enabled
                      and load_timestamp > ('now'::timestamp - '9 weeks'::interval)
                      and extract(dow from load_timestamp) IN(1,2,3,4,5)
                    group by load_host_id, extract(week from load_timestamp)
                    order by 1 ASC,7 DESC""")

    data = defaultdict(list)

    lastRR = None

    for r in cur:

        rr = {'id' : r['id'],
              'avg' : r['avg'],
              'max' : r['max'],
              'min_date' : r['min_date'],
              'max_date' : r['max_date'],
              'trendAvg': 0,
              'trendMax': 0,
              'kw' : r['kw']
              }

        if lastRR != None and lastRR['id']==rr['id']:
            if lastRR['max'] < r['max']:
                lastRR['trendMax'] = -1
            elif lastRR['max'] > r['max']:
                lastRR['trendMax'] = 1

            if lastRR['avg'] < r['avg']:
                lastRR['trendAvg'] = -1
            elif lastRR['avg'] > r['avg']:
                lastRR['trendAvg'] = 1

        data[int(r['id'])].append(rr);
        lastRR = rr

    cur.close()
    conn.close()

    return sorted(data.values(), key = lambda x : hosts.hosts[x[0]['id']]['settings']['uiShortName'])
