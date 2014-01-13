from __future__ import print_function
'''
Created on Sep 20, 2011

@author: jmussler
'''
import hosts
import reportdata
import datetime
import time
import tplE

class PerfTables(object):
    def index(self,**params):
        data = []
        #data.append({'host_name':'Host1','schema_name':'schema1','table_name':'table1','day':'2013-08-20'})
        #print(tplE.env.globals['hosts'])
        if 'show' in params:            
            data = reportdata.getTablePerformanceIssues(params['hostname'], params['from'], params['to'])
            for d in data:
                d['hostuiname'] = hosts.hostIdToUiShortname(d['host_id'])

        interval = {}
        if 'from' in params and 'to' in params:
            interval['from'] = params['from']
            interval['to'] = params['to']
            interval['hostname'] = params['hostname']
        else:
            interval['from'] = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
            interval['to'] = (datetime.datetime.now()).strftime('%Y-%m-%d')
            interval['hostname'] = 'all'
        table = tplE.env.get_template('perf_tables.html')
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return table.render(data=data, interval=interval, host_names=host_names)

    index.exposed = True


class PerfApi(object):
    def index(self,**params):
        data = []
        interval = {}
        #print(tplE.env.globals['hosts'])
        if 'show' in params:            
            data = reportdata.getApiPerformanceIssues(params['hostname'], params['from'], params['to'])
            for d in data:
                d['hostuiname'] = hosts.hostIdToUiShortname(d['host_id'])

        if 'from' in params and 'to' in params:
            interval['from'] = params['from']
            interval['to'] = params['to']
            interval['hostname'] = params['hostname']
        else:
            curdate = datetime.datetime.now()
            interval['from'] = 'r{}_00_{}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=2)).isocalendar()[1])
            interval['to'] = 'r{}_00_{}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=1)).isocalendar()[1])
            interval['hostname'] = 'all'
        table = tplE.env.get_template('perf_api.html')
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return table.render(data=data, interval=interval, host_names=host_names)

    index.exposed = True

class PerfIndexes(object):
    def index(self,**params):
        data = {}
        interval = {}
        if 'show' in params:            
            data = reportdata.getIndexIssues(params['hostname'])
            for s in data:
                for d in data[s]:
                    d['hostuiname'] = hosts.hostIdToUiShortname(d['host_id'])

        if 'hostname' in params:
            interval['hostname'] = params['hostname']
        else:
            interval['hostname'] = 'all'
        table = tplE.env.get_template('perf_indexes.html')
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return table.render(data=data, interval=interval, host_names=host_names)

    index.exposed = True