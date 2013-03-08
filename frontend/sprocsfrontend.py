from __future__ import print_function
'''
Created on Sep 20, 2011

@author: jmussler
'''
import time
import flotgraph
import sprocdata
import hosts
import datetime

from jinja2 import Environment, FileSystemLoader

import tplE

class Show(object):

    def default(self, *p, **params):
            graphcalls= flotgraph.Graph("graphcalls")
            graphcalls.addSeries('Number of calls', 'calls')

            graphtime= flotgraph.TimeGraph("graphruntime")
            graphtime.addSeries('Total run time', 'runtime')

            graphavg= flotgraph.TimeGraph("graphavg")
            graphavg.addSeries('Average run time', 'avg')

            graphavgself= flotgraph.TimeGraph("graphselfavg")
            graphavgself.addSeries('Average self time', 'avgself')

            if(len(p)<=1):
                return """Error: Not enough URL paramter"""

            hostId = p[0]
            name = p[1]

            if len(p) > 2:
                sprocNr = p[2]
            else:
                sprocNr = None

            if 'from' in params and 'to' in params:
                interval = {}
                interval['from'] = params['from']
                interval['to'] = params['to']
            else:
                interval = {}
                interval['from'] = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
                interval['to'] = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

            data = sprocdata.getSingleSprocData( name, hostId , interval, sprocNr)

            for p in data['total_time']:
                    graphtime.addPoint('runtime', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['calls']:
                    graphcalls.addPoint('calls', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['avg_time']:
                    graphavg.addPoint('avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            for p in data['avg_self_time']:
                    graphavgself.addPoint('avgself', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            table = tplE.env.get_template('sproc_detail.html')

            return table.render(hostid = int(hostId),                
                                hostname = hosts.getHostData()[int(hostId)]['settings']['uiLongName'],
                                name = data['name'],
                                interval = interval,
                                graphavg = graphavg.render(),
                                graphselfavg = graphavgself.render(),
                                graphcalls = graphcalls.render(),
                                graphruntime = graphtime.render())

    default.exposed = True

class SprocFrontend(object):
    def all(self,hostId=None):

        if hostId==None:
            return 'Needs hostId';

        sprocs = sprocdata.getSprocsOrderedBy(hostId)
        list = []
        i = 0
        for s in sprocs:
            d = sprocdata.getSingleSprocData(s, hostId, "('now'::timestamp - '4 days'::interval)")
            i += 1
            graph= flotgraph.TimeGraph("graph"+str(i))
            graph.addSeries('Avg.', 'avg')
            print (s)
            print ( len(d['avg_time']) )
            for p in d['avg_time']:
                graph.addPoint('avg', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            list.append( {'graph': graph.render() , 'name': s[0:s.find("(")] , 'i': i } )
        tpl = tplE.env.get_template('all_sprocs.html')
        return tpl.render(graphs=list)

    def __init__(self):
        self.show = Show()

    def index(self):
        return self.default()

    index.exposed = True
    all.exposed = True
