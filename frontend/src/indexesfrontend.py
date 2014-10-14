import flotgraph
import time
import indexdata
import hosts
import datetime

import tplE


class IndexesFrontend(object):

    def default(self, *p, **params):
        if len(p) < 2:
            return ""

        hostId = int(p[0]) if p[0].isdigit() else hosts.uiShortnameToHostId(p[0])
        hostUiName = p[0] if not p[0].isdigit() else hosts.hostIdToUiShortname(p[0])
        table_name = p[1]
        if table_name.find('.') == -1:
            raise Exception('Full table name needed, e.g. schema_x.table_y')
        schema = table_name.split('.')[0]

        if 'from' in params and 'to' in params:
            interval = {}
            interval['from'] = params['from']
            interval['to'] = params['to']
        else:
            interval = {}
            interval['from'] = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
            interval['to'] = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        data = indexdata.getIndexesDataForTable(hostId, table_name, interval['from'], interval['to'])

        all_graphs=[]
        i=0
        for x in data:
            one_index_graphs=[]
            for k,v in x['data'].iteritems():
                i+=1
                if k == 'size':
                    graph = flotgraph.SizeGraph ("index"+str(i),"right")
                else:
                    graph = flotgraph.Graph ("index"+str(i),"right")
                graph.addSeries(k,k)
                for p in v:
                    graph.addPoint(k, int(time.mktime(p[0].timetuple()) * 1000) , p[1])
                graph = graph.render()

                one_index_graphs.append({'data':graph, 'i':i, 'type':k})
                one_index_graphs.sort(key=lambda x:x['type'])
            all_graphs.append({'name':x['index_name'], 'graphs': one_index_graphs, 'last_index_size':x['last_index_size'], 'total_end_size':x['total_end_size'], 'pct_of_total_end_size':x['pct_of_total_end_size']})

        all_graphs = sorted(all_graphs, key=lambda x:x['last_index_size'], reverse=True)

        tpl = tplE.env.get_template('table_indexes.html')
        return tpl.render(table_name=table_name,
                          host=hostId,
                          schema=schema,
                          interval=interval,
                          hostuiname = hostUiName,
                          hostname = hosts.getHosts()[hostId]['uilongname'],
                          all_graphs=all_graphs,
                          target='World')


    def raw(self, host, table, from_date=None, to_date=None):
        host = int(host) if host.isdigit() else hosts.uiShortnameToHostId(host)
        if not from_date:
            from_date = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        return indexdata.getIndexesDataForTable(host, table, from_date, to_date)


    default.exposed = True
