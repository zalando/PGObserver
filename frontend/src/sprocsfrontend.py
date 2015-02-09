from __future__ import print_function
import json
import time
import flotgraph
import sprocdata
import hosts
import datetime
import tplE
import cherrypy


class Show(object):

    def default(self, *p, **params):
        if len(p) == 0:
            return """Error: Not enough URL parameters. Hostname needed"""

        hostId, hostName = hosts.ensureHostIdAndUIShortname(p[0])
        sprocName = None

        if len(p) > 1:
            sprocName = p[1]

        if params.get('search'):
            sprocName = params.get('sproc_search')
            url = '/sprocs/show/' + hostName + '/' + sprocName
            raise cherrypy.HTTPRedirect(cherrypy.url(url))

        interval = {}
        interval['from'] = params.get('from',(datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d'))
        interval['to'] = params.get('to',(datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

        graphcalls= flotgraph.Graph("graphcalls")
        graphcalls.addSeries('Number of calls', 'calls')

        graphtime= flotgraph.TimeGraph("graphruntime")
        graphtime.addSeries('Total run time', 'runtime')

        graphavg= flotgraph.TimeGraph("graphavg")
        graphavg.addSeries('Average run time', 'avg')

        graphavgself= flotgraph.TimeGraph("graphselfavg")
        graphavgself.addSeries('Average self time', 'avgself')

        data = sprocdata.getSingleSprocData(hostId, sprocName, interval)
        if data['name']:    # None if no data for sproc found
            for p in data['total_time']:
                graphtime.addPoint('runtime', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['calls']:
                graphcalls.addPoint('calls', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['avg_time']:
                graphavg.addPoint('avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['avg_self_time']:
                graphavgself.addPoint('avgself', int(time.mktime(p[0].timetuple()) * 1000), p[1])

            sproc_name_wo_params = data['name'] if data['name'].find('(') == -1 else data['name'][0:data['name'].find('(')]
            sproc_params = "" if data['name'].find('(') == -1 else data['name'][data['name'].find('(')+1:-1]

        all_sprocs = sprocdata.getAllActiveSprocNames(hostId)

        table = tplE.env.get_template('sproc_detail.html')
        return table.render(hostid = hostId,
                            hostname = hosts.getHostData()[int(hostId)]['uilongname'],
                            hostuiname = hostName,
                            name_w_params = data['name'] ,
                            params = sproc_params if data['name'] else None,
                            name_wo_params = sproc_name_wo_params if data['name'] else None,
                            interval = interval,
                            sproc_name = sprocName,
                            all_sprocs = all_sprocs,
                            all_sprocs_json = json.dumps(all_sprocs),
                            graphavg = graphavg.render(),
                            graphselfavg = graphavgself.render(),
                            graphcalls = graphcalls.render(),
                            graphruntime = graphtime.render())

    default.exposed = True

class SprocFrontend(object):

    def allgraph(self, hostId):
        hostId, hostUiName = hosts.ensureHostIdAndUIShortname(hostId)
        sprocs = self.get_data(hostId)
        tpl = tplE.env.get_template('all_sprocs.html')
        list = []
        i = 0
        for s in sprocs:
            print ('s')
            print (s)
            d = sprocdata.getSingleSprocData(hostId, s, "('now'::timestamp - '4 days'::interval)")
            i += 1

            graph= flotgraph.TimeGraph("graph"+str(i))
            graph.addSeries('Avg.', 'avg')

            for p in d['avg_time']:
                graph.addPoint('avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            list.append( {'graph': graph.render() , 'name': s[0:s.find("(")] , 'i': i } )
        return tpl.render(graphs=list,
                          hostuiname = hostUiName,
                          hostname = hosts.getHostData()[int(hostId)]['uilongname'],
                          all_sprocs=None)

    def all(self, hostId, graph=False):
        hostId, hostUiName = hosts.ensureHostIdAndUIShortname(hostId)
        graph_list = []
        all_sprocs = None

        if not graph:
           all_sprocs = sprocdata.getAllActiveSprocNames(hostId)
        else:
            sprocs = self.get_data(hostId)

            i = 0
            for s in sprocs:
                print ('s')
                print (s)
                d = sprocdata.getSingleSprocData(hostId, s, "('now'::timestamp - '4 days'::interval)")
                i += 1

                graph= flotgraph.TimeGraph("graph"+str(i))
                graph.addSeries('Avg.', 'avg')

                for p in d['avg_time']:
                    graph.addPoint('avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

                graph_list.append( {'graph': graph.render() , 'name': s[0:s.find("(")] , 'i': i } )

        tpl = tplE.env.get_template('all_sprocs.html')
        return tpl.render(graphs=graph_list,
                          hostuiname = hostUiName,
                          hostname = hosts.getHostData()[int(hostId)]['uilongname'],
                          all_sprocs = all_sprocs)

    def __init__(self):
        self.show = Show()

    def index(self):
        return self.default()

    def get_data(self, hostId):
        if hostId==None:
            return 'Needs valid hostId/uiShortName'

        sprocs = sprocdata.getActiveSprocsOrderedBy(hostId)
        return sprocs

    index.exposed = True
    all.exposed = True
