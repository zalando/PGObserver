from __future__ import print_function
import reportdata
import datetime
import time
import tplE
import cherrypy
import flotgraph
import hosts
import datadb


class PerfTables(object):
    def index(self, **params):
        data, interval, uishortnames = self.get_data(**params)
        table = tplE.env.get_template('perf_tables.html')
        return table.render(data=data, interval=interval, uishortnames=uishortnames)

    def raw(self, uishortname, from_date, to_date):
        data, interval, uishortnames = self.get_data(show='show', hostname=uishortname, **{'from': from_date, 'to': to_date})
        return data

    def get_data(self, **params):
        data = []
        if 'show' in params:
            uishortname = params.get('uishortname')
            if uishortname == 'all':
                uishortname = None
            data = reportdata.getTablePerformanceIssues(uishortname, params['from'], params['to'])

        interval = {}
        if 'from' in params and 'to' in params:
            interval['from'] = params['from']
            interval['to'] = params['to']
            interval['uishortname'] = params['uishortname']
        else:
            interval['from'] = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
            interval['to'] = (datetime.datetime.now()).strftime('%Y-%m-%d')
            interval['uishortname'] = 'all'

        return data, interval, hosts.getAllHostUinamesSorted()

    index.exposed = True


class PerfApi(object):
    def index(self,**params):
        data, interval, uishortnames = self.get_data(**params)
        table = tplE.env.get_template('perf_api.html')
        return table.render(data=data, interval=interval, uishortnames=uishortnames)

    def raw(self, uishortname, from_version, to_version):
        data, interval, uishortnames = self.get_data(uishortname=uishortname, show='show', **{'from': from_version, 'to': to_version})
        return data

    def get_data(self, **params):
        data = []
        interval = {}
        if 'show' in params:
            uishortname = params.get('uishortname')
            if uishortname == 'all':
                uishortname = None
            data = reportdata.getApiPerformanceIssues(uishortname, params['from'], params['to'])

        if 'from' in params and 'to' in params:
            interval['from'] = params['from']
            interval['to'] = params['to']
            interval['uishortname'] = params['uishortname']
        else:
            curdate = datetime.datetime.now()
            interval['from'] = 'r{}_00_{:02}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=2)).isocalendar()[1])
            interval['to'] = 'r{}_00_{:02}'.format(curdate.strftime('%y'), (datetime.datetime.now()-datetime.timedelta(weeks=1)).isocalendar()[1])
            interval['uishortname'] = 'all'
        uishortnames = hosts.getAllHostUinamesSorted()
        return data, interval, uishortnames

    index.exposed = True


class PerfIndexes(object):
    def index(self,**params):
        data, interval, uishortnames = None, None, None
        hot_queries_allowed = tplE._settings.get('allow_hot_queries', True)
        if hot_queries_allowed:
            data, interval, uishortnames = self.get_data(**params)
        table = tplE.env.get_template('perf_indexes.html')
        return table.render(data=data, interval=interval, uishortnames=uishortnames, hot_queries_allowed=hot_queries_allowed)

    def raw(self, uishortname='all'):
        data, interval, uishortnames = self.get_data(uishortname=uishortname, show='show')
        return data

    def get_data(self, **params):
        data = []
        interval = {}
        if 'show' in params:
            data = reportdata.getIndexIssues(params.get('uishortname'))

        if 'uishortname' in params:
            interval['uishortname'] = params['uishortname']
        else:
            interval['uishortname'] = 'all'

        uishortnames = hosts.getAllHostUinamesSorted()
        return data, interval, uishortnames

    index.exposed = True


class PerfUnusedSchemas(object):

    def index(self, uishortname=None, **params):
        data, from_date, to_date, uishortnames, filter = self.get_data(uishortname=uishortname, **params)
        if 'download' in params:
            return self.getdropschemasql(uishortname, from_date, to_date, filter)
        table = tplE.env.get_template('perf_schemas.html')
        return table.render(data=data, from_date=from_date, to_date=to_date, uishortname=uishortname,
                            uishortnames=uishortnames, filter=filter)

    def raw(self, uishortname='all', from_date=None, to_date=None):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        data, from_date, to_date, uishortnames, filter = self.get_data(uishortname=uishortname, show='show', **span)
        return data

    def get_data(self, uishortname, **params):
        data = {}
        filter = params.get('filter', 'api')
        from_date = params.get('from_date', (datetime.datetime.now() - datetime.timedelta(14)).strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))

        if uishortname:
            data = reportdata.get_unused_schemas(uishortname, from_date, to_date, filter)

        uishortnames = hosts.getHostsWithFeatureAsShortnames('schemaStatsGatherInterval')

        return data, from_date, to_date, uishortnames, filter

    def getdropschemasql(self, uishortname, from_date=None, to_date=None, filter=''):
        if from_date is None:
            from_date = (datetime.datetime.now() - datetime.timedelta(14)).strftime('%Y-%m-%d')
        if to_date is None:
            to_date = (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d')
        cherrypy.response.headers['content-type'] = 'text/csv; charset=utf-8'
        cherrypy.response.headers['content-disposition'] = 'attachment; filename=schema_drops_' + uishortname + '_' + datetime.datetime.now().strftime('%y-%m-%d_%H%M') + '.sql'
        return reportdata.get_unused_schemas_drop_sql(uishortname, from_date, to_date, filter)

    def detailed(self, uishortname=None, **params):
        schemagraphs = []
        from_date = params.get('from_date', (datetime.datetime.now() - datetime.timedelta(7)).strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))
        filter = params.get('filter', '')

        if uishortname:
            data = reportdata.get_schema_usage_for_host(uishortname, from_date, to_date, filter)
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

        uishortnames = hosts.getHostsWithFeatureAsShortnames('schemaStatsGatherInterval')
        table = tplE.env.get_template('perf_schemas_detailed.html')
        return table.render(schemagraphs=schemagraphs, from_date=from_date, to_date=to_date,
                            uishortname=uishortname, uishortnames=uishortnames, filter=filter)

    index.exposed = True
    getdropschemasql.exposed = True
    detailed.exposed = True


def is_sproc_installed(sproc_name):
    sql = """select * from pg_proc where proname = %s"""
    return datadb.execute(sql, (sproc_name,))


class PerfLocksReport(object):
    def index(self, uishortname='all', **params):
        data, from_date, to_date, uishortnames = self.get_data(uishortname, **params)
        table = tplE.env.get_template('perf_locks.html')
        return table.render(data=data, from_date=from_date, to_date=to_date, uishortname=uishortname, uishortnames=uishortnames)

    index.exposed = True

    def raw(self, from_date=None, to_date=None):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        data, from_date, to_date, uishortnames = self.get_data(show='show', **span)
        return data

    def get_data(self, uishortname, **params):
        data = []
        from_date = params.get('from_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))

        if 'show' in params:
            if not is_sproc_installed('blocking_last_day_by_shortname'):
                raise Exception('Required additional module is not installed, see - https://github.com/zalando/PGObserver/blob/master/extra_features/blocking_monitor/FEAT_DESC.md')
            if uishortname == 'all':
                uishortname = None
            data = reportdata.getLocksReport(uishortname, from_date, to_date)
        uishortnames = hosts.getHostsWithFeatureAsShortnames('blockingStatsGatherInterval')
        return data, from_date, to_date, uishortnames


class PerfStatStatementsReport(object):

    def index(self, **params):
        uishortname, uishortnames, data, from_date, to_date, order_by, limit, no_copy_ddl, min_calls = self.get_data(**params)
        table = tplE.env.get_template('perf_stat_statements.html')
        return table.render(uishortname=uishortname, uishortnames=uishortnames,
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

    def raw(self, uishortname, from_date=None, to_date=None, order_by='1', limit='50'):
        span = {}
        if from_date is not None:
            span['from_date'] = from_date
        if to_date is not None:
            span['to_date'] = to_date
        uishortname, uishortnames, data, from_date, to_date, order_by, limit, no_copy_ddl, min_calls = self.get_data(show='show', uishortname=uishortname, orderby=order_by, limit=limit, **span)
        return data

    def get_data(self, **params):
        data = []
        uishortname = params.get('uishortname', '')
        order_by = params.get('order_by', '1')
        limit = params.get('limit', '50')
        from_date = params.get('from_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        to_date = params.get('to_date', (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d'))
        no_copy_ddl = params.get('no_copy_ddl', True)
        min_calls = params.get('min_calls', '3')

        if 'show' in params and uishortname:
            data = reportdata.getStatStatements(uishortname, from_date, to_date, order_by, limit, no_copy_ddl, min_calls)
        for d in data:
            d['query_short'] = d['query'][:60].replace('\n',' ').replace('\t',' ') + ('...' if len(d['query']) > 60 else '')

        uishortnames = hosts.getHostsWithFeatureAsShortnames('statStatementsGatherInterval')
        return uishortname, uishortnames, data, from_date, to_date, order_by, limit, no_copy_ddl, min_calls


class PerfBloat(object):
    def index(self, uishortname=None, **params):
        data, uishortnames, bloat_type, order_by, limit = [None] * 5
        hot_queries_allowed = tplE._settings.get('allow_hot_queries', True)
        if hot_queries_allowed:
            data, uishortnames, bloat_type, order_by, limit = self.get_data(uishortname=uishortname, **params)
        table = tplE.env.get_template('perf_bloat.html')
        return table.render(uishortname=uishortname, uishortnames=uishortnames, bloat_type=bloat_type, order_by=order_by,
                            limit=limit, data=data, hot_queries_allowed=hot_queries_allowed)

    def raw(self, uishortname=None, **params):
        data, uishortnames, bloat_type, order_by, limit = self.get_data(uishortname, **params)
        return data

    def get_data(self, uishortname=None, **params):
        uishortnames = hosts.getAllHostUinamesSorted()
        bloat_type = params.get('bloat_type', 'table')
        order_by = params.get('order_by', 'wasted_bytes')
        limit = params.get('limit', '50')
        data = []
        if uishortname:
            if bloat_type == 'table':
                msg, data = reportdata.getBloatedTablesForHostname(uishortname, order_by, limit)
            else:
                msg, data = reportdata.getBloatedIndexesForHostname(uishortname, order_by, limit)
            if msg:
                raise Exception('Failed to get data: ' + msg)

        return data, uishortnames, bloat_type, order_by, limit

    index.exposed = True