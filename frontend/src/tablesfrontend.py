import flotgraph
import time
import tabledata
import hosts
import datetime
import tplE
import cherrypy


class ShowTable(object):

    def default(self, *p, **params):
        if len(p) < 2:
            return ""
        hostId = int(p[0]) if p[0].isdigit() else hosts.uiShortnameToHostId(p[0])
        hostUiName = p[0] if not p[0].isdigit() else hosts.hostIdToUiShortname(p[0])
        name = p[1]
        interval = {}

        if 'interval' in params:
            interval['interval'] = str(params['interval'])+' days'
        elif 'from' in params and 'to' in params:
            interval['from'] = params['from']
            interval['to'] = params['to']
        else:
            days = int(cherrypy.request.cookie['days'].value) if 'days' in cherrypy.request.cookie else 8
            interval['from'] = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
            interval['to'] = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        data = tabledata.getTableData(hostId, name, interval)

        graph_table_size = flotgraph.SizeGraph ("graphtablesize","right")
        graph_table_size.addSeries("Table Size","size")
        for p in data['table_size']:
            graph_table_size.addPoint("size", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_index_size = flotgraph.SizeGraph ("graphindexsize","right")
        graph_index_size.addSeries("Index Size", "size")
        for p in data['index_size']:
            graph_index_size.addPoint("size", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_seq_scans = flotgraph.Graph ("graphseqscans","right")
        graph_seq_scans.addSeries("Sequential Scans","count")
        for p in data['seq_scans']:
            graph_seq_scans.addPoint("count", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_index_scans = flotgraph.Graph ("graphindexscans","right")
        graph_index_scans.addSeries("Index Scans","count")
        for p in data['index_scans']:
            graph_index_scans.addPoint("count", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_t_ins = flotgraph.Graph ("gtupins","right")
        graph_t_ins.addSeries("Inserts","count",'#FF0000')
        for p in data['ins']:
            graph_t_ins.addPoint("count", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_t_upd = flotgraph.Graph ("gtupupd","right")
        graph_t_upd.addSeries("Updates","count",'#FF8800')
        graph_t_upd.addSeries("Hot Updates","hotcount",'#885500')
        for p in data['upd']:
            graph_t_upd.addPoint("count", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        for p in data['hot']:
            graph_t_upd.addPoint("hotcount", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_t_del = flotgraph.Graph ("gtupdel","right")
        graph_t_del.addSeries("Deletes","count")
        for p in data['del']:
            graph_t_del.addPoint("count", int(time.mktime(p[0].timetuple()) * 1000) , p[1])


        data = tabledata.getTableIOData(hostId, name, interval)

        graph_index_iob = flotgraph.Graph ("graphindexiob","right")
        graph_index_iob.addSeries("Index_hit","ihit")
        for p in data['index_hit']:
            graph_index_iob.addPoint("ihit", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_index_iod = flotgraph.Graph ("graphindexiod","right")
        graph_index_iod.addSeries("Index_read","iread",'#FF0000')
        for p in data['index_read']:
            graph_index_iod.addPoint("iread", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_heap_iod = flotgraph.Graph ("graphheapiod","right")
        graph_heap_iod.addSeries("Heap_read","hread",'#FF0000')
        for p in data['heap_read']:
            graph_heap_iod.addPoint("hread", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        graph_heap_iob = flotgraph.Graph ("graphheapiob","right")
        graph_heap_iob.addSeries("Heap_hit","hhit")
        for p in data['heap_hit']:
            graph_heap_iob.addPoint("hhit", int(time.mktime(p[0].timetuple()) * 1000) , p[1])

        tpl = tplE.env.get_template('table_detail.html')
        return tpl.render(name=name,
                          schema_name=name[:name.find('.')],
                          host=hostId,
                          interval=interval,
                          hostuiname = hostUiName,
                          hostname = hosts.getHosts()[hostId]['uilongname'],
                          graphtablesize=graph_table_size.render(),
                          graphindexsize=graph_index_size.render(),
                          graphseqscans=graph_seq_scans.render(),
                          graphindexscans=graph_index_scans.render(),

                          graphindexiod=graph_index_iod.render(),
                          graphindexiob=graph_index_iob.render(),
                          graphheapiod=graph_heap_iod.render(),
                          graphheapiob=graph_heap_iob.render(),

                          gtupins=graph_t_ins.render(),
                          gtupupd=graph_t_upd.render(),
                          gtupdel=graph_t_del.render(),

                          target='World')

    default.exposed = True


class TableFrontend(object):
    def __init__(self):
        self.show = ShowTable()

    def index(self, order='2', date_from=None, date_to=None, limit=50, show=None):
        """ top tables over all dbs """
        order = int(order)
        if not date_from:
            date_from = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        top_tables = []
        top_tables = tabledata.getTopTables(None, date_from, date_to, order, limit)
        table = tplE.env.get_template('tables_global_top.html')
        return table.render(list=top_tables,
                            limit=limit, order=order, date_from=date_from, date_to=date_to)

    def allgraph(self):
        systems = self.get_all_databases_size()
        tmpl = tplE.env.get_template('tables.html')
        return tmpl.render(systems=sorted(systems,key=lambda x : x['h']['uishortname']),
                           target='World')

    def get_all_databases_size(self):   # show a graph of all db sizes?
        size = tabledata.getDatabaseSizes()

        systems = []

        hs = hosts.getHostData().values()

        for h in hs:
            g = flotgraph.SizeGraph("s" + str(h['host_id']))
            tabledata.fillGraph(g,size[h['host_id']])

            s = self.renderSizeTable(h['host_id'])  # TODO doesnt exist
            systems.append({ 'id' : "s"+str(h['host_id']) , 't' : s , 'g' : g.render() , 'h' : h })
        return systems


    def alltables(self, hostId, date_from=None, date_to=None, order=None, pattern="", **params):
        tpl = tplE.env.get_template('all_tables.html')
        hostUiName = hostId if not hostId.isdigit() else hosts.hostIdToUiShortname(hostId)

        if not date_from:
            date_from = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        hostId, hostname, top_tables, order = self.get_alltables_data(hostId, date_from, date_to, order, pattern)

        total_size = 0
        total_index_size = 0
        for t in top_tables:
            total_size += t['table_size']
            total_index_size += t['index_size']

        total_size = tabledata.makePrettySize(total_size)
        total_index_size = tabledata.makePrettySize(total_index_size)

        return tpl.render(hostname=hostname, hostuiname=hostUiName, order=int(order), list=top_tables,
                          date_from=date_from, date_to=date_to, pattern=pattern,total_size=total_size, total_index_size=total_index_size)

    def raw_alltables(self, host, date_from, date_to, order=None):
        hostId, hostname, top_tables, order = self.get_alltables_data(host, date_from, date_to, order)
        return top_tables

    def get_alltables_data(self, hostId, date_from, date_to, order=None, pattern=None):
        hostId = hostId if hostId.isdigit() else hosts.uiShortnameToHostId(hostId)

        if hostId is None:
            return 'valid hostId/hostUiShortname expected'
        if order==None:
            order=2
        hostname = hosts.getHostData()[int(hostId)]['uilongname']
        top_tables = tabledata.getTopTables(hostId, date_from, date_to, order, None, pattern)
        return hostId, hostname, top_tables, order

    def default(self):
        return ""


    alltables.exposed = True
    default.exposed = True
    allgraph.exposed = True
    index.exposed = True
