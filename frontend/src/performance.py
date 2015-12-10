from __future__ import print_function
import reportdata
import datetime
import time
import tplE
import cherrypy
import flotgraph
import hosts


class PerfTables(object):
    def index(self,**params):
        data, interval, host_names = self.get_data(**params)
        table = tplE.env.get_template('perf_tables.html')
        return table.render(data=data, interval=interval, host_names=host_names)

    def raw(self, hostname, from_date, to_date):
        data, interval, host_names = self.get_data(show='show', hostname=hostname, **{'from': from_date, 'to': to_date})
        return data

    def get_data(self, **params):
        data = []
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
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return data, interval, host_names

    index.exposed = True


class PerfApi(object):
    def index(self,**params):
        data, interval, host_names = self.get_data(**params)
        table = tplE.env.get_template('perf_api.html')
        return table.render(data=data, interval=interval, host_names=host_names)

    def raw(self, hostname, from_version, to_version):
        data, interval, host_names = self.get_data(hostname=hostname, show='show', **{'from': from_version, 'to': to_version})
        return data

    def get_data(self, **params):
        data = []
        interval = {}
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
            interval['from'] = 'r{}_00_{:02}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=2)).isocalendar()[1])
            interval['to'] = 'r{}_00_{:02}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=1)).isocalendar()[1])
            interval['hostname'] = 'all'
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return data, interval, host_names

    index.exposed = True


class PerfIndexes(object):
    def index(self,**params):
        data, interval, host_names = None, None, None
        hot_queries_allowed = tplE._settings.get('allow_hot_queries', True)
        if hot_queries_allowed:
            data, interval, host_names = self.get_data(**params)
        table = tplE.env.get_template('perf_indexes.html')
        return table.render(data=data, interval=interval, host_names=host_names, hot_queries_allowed=hot_queries_allowed)

    def raw(self, hostname='all'):
        data, interval, host_names = self.get_data(hostname=hostname, show='show')
        return data

    def get_data(self, **params):
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
        host_names = sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return data, interval, host_names

    index.exposed = True


class PerfUnusedSchemas(object):

    def index(self, selected_hostname=None, **params):
        data, from_date, to_date, host_names, filter = self.get_data(selected_hostname=selected_hostname, **params)
        if 'download' in params:
            return self.getdropschemasql(selected_hostname, from_date, to_date, filter)
        table = tplE.env.get_template('perf_schemas.html')
        return table.render(data=data, from_date=from_date, to_date=to_date, selected_hostname=selected_hostname,
                            host_names=host_names, filter=filter)

    def raw(self, hostname='all', from_date=None, to_date=None):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        data, from_date, to_date, host_names, filter = self.get_data(selected_hostname=hostname, show='show', **span)
        return data

    def get_data(self, selected_hostname, **params):
        data = {}
        filter = params.get('filter', 'api')
        from_date = params.get('from_date', (datetime.datetime.now() - datetime.timedelta(14)).strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))

        if selected_hostname:
            data = reportdata.get_unused_schemas(selected_hostname, from_date, to_date, filter)

        host_names = hosts.getHostsWithFeature('schemaStatsGatherInterval').items()
        host_names.sort(key=lambda x: x[1]['host_name'])

        return data, from_date, to_date, host_names, filter

    def getdropschemasql(self, host_name, from_date=None, to_date=None, filter=''):
        if from_date is None:
            from_date = (datetime.datetime.now() - datetime.timedelta(14)).strftime('%Y-%m-%d')
        if to_date is None:
            to_date = (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d')
        cherrypy.response.headers['content-type'] = 'text/csv; charset=utf-8'
        cherrypy.response.headers['content-disposition'] = 'attachment; filename=schema_drops_' + host_name + '_' + datetime.datetime.now().strftime('%y-%m-%d_%H%M') + '.sql'
        return reportdata.get_unused_schemas_drop_sql(host_name, from_date, to_date, filter)

    def detailed(self, selected_hostname=None, **params):
        schemagraphs = []
        from_date = params.get('from_date', (datetime.datetime.now() - datetime.timedelta(7)).strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))
        filter = params.get('filter', '')

        if selected_hostname:
            if selected_hostname not in hosts.getAllHostNames():
                selected_hostname = hosts.uiShortNameToHostName(selected_hostname)
            data = reportdata.get_schema_usage_for_host(selected_hostname, from_date, to_date, filter)
            for schema_name, data in data.iteritems():
                g_calls = flotgraph.Graph (schema_name + "_calls")
                g_calls.addSeries('Sproc calls', 'calls')
                g_iud = flotgraph.Graph (schema_name + "_iud")
                g_iud.addSeries('IUD', 'iud')
                g_scans = flotgraph.Graph (schema_name + "_scans")
                g_scans.addSeries('Seq+Ind Scans', 'scans')
                for p in data:
                    g_calls.addPoint('calls', int(time.mktime(p[0].timetuple()) * 1000) , p[1][0])
                    g_iud.addPoint('iud', int(time.mktime(p[0].timetuple()) * 1000) , p[1][2]+p[1][3]+p[1][4])
                    g_scans.addPoint('scans', int(time.mktime(p[0].timetuple()) * 1000) , p[1][1])
                schemagraphs.append((schema_name, [g_calls.render(), g_iud.render(), g_scans.render()]))

        table = tplE.env.get_template('perf_schemas_detailed.html')
        return table.render(schemagraphs=schemagraphs, from_date=from_date, to_date=to_date, selected_hostname=selected_hostname, host_names=hosts.getAllHostNames(), filter=filter)


    index.exposed = True
    getdropschemasql.exposed = True
    detailed.exposed = True


class PerfLocksReport(object):
    def index(self, hostname='all', **params):
        data, from_date, to_date, host_names = self.get_data(hostname, **params)
        table = tplE.env.get_template('perf_locks.html')
        return table.render(data=data, from_date=from_date, to_date=to_date, hostname=hostname, host_names=host_names)

    index.exposed = True


    def raw(self, from_date=None, to_date=None):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        data, from_date, to_date, host_names = self.get_data(show='show', **span)
        return data

    def get_data(self, hostname, **params):
        data = []
        from_date = params.get('from_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))

        if 'show' in params:
            data = reportdata.getLocksReport(hostname, from_date, to_date)
        host_names = hosts.getHostsWithFeature('blockingStatsGatherInterval').items()
        return data, from_date, to_date, host_names


class PerfStatStatementsReport(object):
    def index(self,**params):
        hostname, host_names, hostuiname, data, from_date, to_date, order_by, limit, no_copy_ddl, min_calls = self.get_data(**params)
        table = tplE.env.get_template('perf_stat_statements.html')
        return table.render(hostname=hostname, hostuiname=hostuiname, host_names=host_names,
                            data=data, from_date=from_date, to_date=to_date,
                            order_by=order_by, limit=limit, no_copy_ddl=no_copy_ddl, min_calls=min_calls)

    index.exposed = True

    def graph(self, hostuiname, query_id, **params):
        hostid, hostuiname = hosts.ensureHostIdAndUIShortname(hostuiname)
        from_date = params.get('from_date', (datetime.datetime.now() -  datetime.timedelta(1)).strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))

        data = reportdata.getStatStatementsGraph(hostid, query_id, from_date, to_date)

        graphcalls = flotgraph.Graph("graphcalls")
        graphcalls.addSeries('Number of calls', 'calls')
        graphavgruntime = flotgraph.TimeGraph("graphavgruntime")
        graphavgruntime.addSeries('Avg. runtime', 'avgruntime')
        graphruntime = flotgraph.TimeGraph("graphruntime")
        graphruntime.addSeries('Runtime', 'runtime')
        graphblocksread = flotgraph.Graph("graphblocksread")
        graphblocksread.addSeries('Blocks read', 'blocksread')
        graphblocksread.addSeries('Temp blocks read', 'tempblocksread', '#FFFF00')
        graphblockswritten = flotgraph.Graph("graphblockswritten")
        graphblockswritten.addSeries('Blocks written', 'blockswritten')
        graphblockswritten.addSeries('Temp blocks written', 'tempblockswritten', '#FFFF00')

        prev_row = None
        for d in data:
            if prev_row is None:
                prev_row = d
                continue
            calls = d['calls'] - prev_row['calls']
            graphcalls.addPoint('calls', int(time.mktime(d['timestamp'].timetuple()) * 1000), calls)
            runtime = d['total_time'] - prev_row['total_time']
            graphruntime.addPoint('runtime', int(time.mktime(d['timestamp'].timetuple()) * 1000), runtime)
            avg_runtime = round(runtime / float(calls), 2) if calls > 0 else 0
            graphavgruntime.addPoint('avgruntime', int(time.mktime(d['timestamp'].timetuple()) * 1000), avg_runtime)
            graphblocksread.addPoint('blocksread', int(time.mktime(d['timestamp'].timetuple()) * 1000), d['blks_read'] - prev_row['blks_read'])
            graphblocksread.addPoint('tempblocksread', int(time.mktime(d['timestamp'].timetuple()) * 1000), d['temp_blks_read'] - prev_row['temp_blks_read'])
            graphblockswritten.addPoint('blockswritten', int(time.mktime(d['timestamp'].timetuple()) * 1000), d['blks_written'] - prev_row['blks_written'])
            graphblockswritten.addPoint('tempblockswritten', int(time.mktime(d['timestamp'].timetuple()) * 1000), d['temp_blks_written'] - prev_row['temp_blks_written'])

            prev_row = d

        table = tplE.env.get_template('perf_stat_statements_detailed.html')
        return table.render(hostuiname=hostuiname,
                            query = prev_row['query'],
                            query_id = prev_row['query_id'],
                            graphcalls=graphcalls.render(),
                            graphavgruntime=graphavgruntime.render(),
                            graphruntime=graphruntime.render(),
                            graphblocksread=graphblocksread.render(),
                            graphblockswritten=graphblockswritten.render(),
                            from_date=from_date, to_date=to_date)

    graph.exposed = True


    def raw(self, hostname, from_date=None, to_date=None, order_by='1', limit='50'):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        hostname, host_names, data, from_date, to_date, order_by, limit = self.get_data(show='show', hostname=hostname, orderby=order_by, limit=limit, **span)
        return data

    def get_data(self, **params):
        data = {}
        hostname = params.get('hostname')
        hostuiname = ''
        order_by = params.get('order_by', '1')
        limit = params.get('limit', '50')
        from_date = params.get('from_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))
        no_copy_ddl = params.get('no_copy_ddl', True)
        min_calls = params.get('min_calls', '3')

        if 'show' in params and hostname:
            data = reportdata.getStatStatements(hostname, from_date, to_date, order_by, limit, no_copy_ddl, min_calls)
            hostuiname = hosts.getHostUIShortnameByHostname(hostname)
        for d in data:
            d['query_short'] = d['query'][:60].replace('\n',' ').replace('\t',' ') + ('...' if len(d['query']) > 60 else '')

        host_names=sorted(hosts.hosts.items(), key = lambda h : h[1]['host_name'])
        return hostname, host_names, hostuiname, data, from_date, to_date, order_by, limit, no_copy_ddl, min_calls


class PerfBloat(object):
    def index(self, selected_hostname=None, **params):
        data, hostnames, bloat_type, order_by, limit, hostuiname = [None] * 6
        hot_queries_allowed = tplE._settings.get('allow_hot_queries', True)
        if hot_queries_allowed:
            data, hostnames, bloat_type, order_by, limit, hostuiname = self.get_data(selected_hostname=selected_hostname, **params)
        table = tplE.env.get_template('perf_bloat.html')
        return table.render(selected_hostname=selected_hostname, hostnames=hostnames, bloat_type=bloat_type, order_by=order_by,
                            limit=limit, data=data, hostuiname=hostuiname, hot_queries_allowed=hot_queries_allowed)

    def raw(self, selected_hostname=None, **params):
        data, hostnames, bloat_type, order_by, limit, hostuiname = self.get_data(selected_hostname, **params)
        return data

    def get_data(self, selected_hostname=None, **params):
        hostnames = hosts.getAllHostNames()
        bloat_type = params.get('bloat_type', 'table')
        order_by = params.get('order_by', 'wasted_bytes')
        limit = params.get('limit', '50')
        data = []
        hostuiname = None
        if selected_hostname:
            if bloat_type == 'table':
                msg, data = reportdata.getBloatedTablesForHostname(selected_hostname, order_by, limit)
            else:
                msg, data = reportdata.getBloatedIndexesForHostname(selected_hostname, order_by, limit)
            if msg:
                raise Exception('Failed to get data: ' + msg)
            hostuiname = hosts.getHostUIShortnameByHostname(selected_hostname)

        return data, hostnames, bloat_type, order_by, limit, hostuiname

    index.exposed = True