from __future__ import print_function
import time
import flotgraph
import hosts
import reportdata
import tplE


class Report(object):
    def index(self, hostId=None):
        if hostId is not None and not hostId.isdigit():
            hostId = hosts.uiShortnameToHostId(hostId)

        weeks = 10
        data = reportdata.getLoadReportData(hostId, weeks-1)

        graph_load = []
        graph_wal = []

        print ('hostId')
        print (hostId)
        if hostId:
            graph_data = reportdata.getLoadReportDataDailyAvg(hostId, weeks-1)
            print (graph_data)

            graph_load = flotgraph.Graph('graph_load', 'left', 30)
            graph_load.addSeries('CPU Load daily avg.', 'cpu')
            graph_wal = flotgraph.SizeGraph('graph_wal')
            graph_wal.addSeries('WAL daily avg.', 'wal')

            for p in graph_data:
                graph_load.addPoint('cpu', int(time.mktime(p['date'].timetuple()) * 1000), p['cpu_15'])
                graph_wal.addPoint('wal', int(time.mktime(p['date'].timetuple()) * 1000), p['wal_b'])

            graph_load = graph_load.render()
            graph_wal = graph_wal.render()

        table = tplE.env.get_template('report_basic.html')
        return table.render(hosts=hosts.hosts, data=data, graph_load=graph_load, graph_wal=graph_wal, weeks=weeks)

    index.exposed = True

    def raw(self, host=None):
        if host is None:
            host_id = None
        elif host.isdigit():
            host_id = host
        else:
            host_id = hosts.uiShortnameToHostId(host)
        return reportdata.getLoadReportData(host_id)


