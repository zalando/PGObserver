from __future__ import print_function
'''
Created on Sep 28, 2011

@author: jmussler
'''
import topsprocs
import flotgraph
import time
import tabledata
import hosts
import tplE
import sprocdata

class MonitorFrontend(object):

        def __init__(self, hostId = 1):
            self.hostId = hostId

        def default(self, hostId = None, limit=10):
            hostUiName = None
            if hostId == None:
                hostId = self.hostId

            if str(hostId).isdigit():
                hostId = int(hostId)
                hostUiName = hosts.hostIdToUiShortname(hostId)
            else:
                hostUiName = hostId
                hostId = int(hosts.uiShortnameToHostId(hostId))

            load = topsprocs.getLoad(hostId)
            cpuload = topsprocs.getCpuLoad(hostId)
            walvolumes = topsprocs.getWalVolumes(hostId)

            graph1 = flotgraph.Graph("graph1","left",30)
            graph_wal = flotgraph.Graph("graph_wal","left",30)

            graph1.addSeries('CPU Load 15min avg','acpu_15min_avg','#FF0000')
            for p in cpuload['load_15min_avg']:
                graph1.addPoint('acpu_15min_avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            graph1.addSeries('Sproc Load 15 min', 'load_15min')
            for p in load['load_15min']:
                graph1.addPoint('load_15min', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            graph_wal.addSeries('WAL vol. 15 min (in MB)', 'wal_15min')
            for p in walvolumes['wal_15min_growth']:
                graph_wal.addPoint('wal_15min', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            sizes = tabledata.getDatabaseSizes(hostId)

            graph_size = flotgraph.SizeGraph("graph_size")
            if hostId in sizes:
                tabledata.fillGraph(graph_size,sizes[hostId])

            taggedload = sprocdata.getSprocDataByTags()

            graphT = flotgraph.BarGraph("graphtag","left",30)
            graphT.addSeries('Articles','article','#FF0000')
            graphT.addSeries('Stock','stock','#0000FF')
            graphT.addSeries('Export','export','#00FF00')
            graphT.addSeries('get_catalog_article','get_article','#00FFFF')

            for p in taggedload[1]:
                graphT.addPoint('article',int(time.mktime(p[0].timetuple()) * 1000), p[1])

            for p in taggedload[2]:
                graphT.addPoint('stock',int(time.mktime(p[0].timetuple()) * 1000), p[1])

            for p in taggedload[3]:
                graphT.addPoint('export',int(time.mktime(p[0].timetuple()) * 1000), p[1])

            for p in taggedload[4]:
                graphT.addPoint('get_article',int(time.mktime(p[0].timetuple()) * 1000), p[1])

            tmpl = tplE.env.get_template('index.html')
            return tmpl.render(hostid=hostId,
                               hostuiname=hostUiName,
                               graph1=graph1.render(),
                               graph_wal=graph_wal.render(),
                               graph_size=graph_size.render(),
                               graphtag = graphT.render(),
                               limit=limit,

                               #top10alltimesavg = self.renderTop10AllTime(topsprocs.avgRuntimeOrder),
                               top10hours1avg = self.renderTop10LastHours(topsprocs.avgRuntimeOrder,1, hostId,limit),
                               top10hours3avg = self.renderTop10LastHours(topsprocs.avgRuntimeOrder,3, hostId,limit),

                               #top10alltimestotal = self.renderTop10AllTime(topsprocs.totalRuntimeOrder),
                               top10hours1total = self.renderTop10LastHours(topsprocs.totalRuntimeOrder,1, hostId,limit),
                               top10hours3total = self.renderTop10LastHours(topsprocs.totalRuntimeOrder,3, hostId,limit),

                               #top10alltimescalls = self.renderTop10AllTime(topsprocs.totalCallsOrder),
                               top10hours1calls = self.renderTop10LastHours(topsprocs.totalCallsOrder,1, hostId,limit),
                               top10hours3calls = self.renderTop10LastHours(topsprocs.totalCallsOrder,3, hostId,limit),

                               target='World')


        def index(self,limit=10):
            return self.default(1,limit)

        def renderTop10AllTime(self, order):
            table = tplE.env.get_template('table.html')
            return table.render(list=topsprocs.getTop10AllTimes(order))

        def renderTop10LastHours(self, order, hours=1, hostId = 1, limit = 10):
            table = tplE.env.get_template('table.html')
            return table.render(hostid = hostId, hostuiname=hosts.hostIdToUiShortname(hostId), list=topsprocs.getTop10LastXHours(order, hours, hostId,limit))

        index.exposed = False
        default.exposed = True
