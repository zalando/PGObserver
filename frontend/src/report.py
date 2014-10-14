from __future__ import print_function
import hosts
import reportdata
import tplE


class Report(object):
    def index(self, hostId=None):
        if hostId is not None and not hostId.isdigit():
            hostId = hosts.uiShortnameToHostId(hostId)

        data = reportdata.getLoadReportData(hostId)

        table = tplE.env.get_template('report_basic.html')
        return table.render(hosts=hosts.hosts, data=data)

    index.exposed = True

    def raw(self, host=None):
        if host is None:
            host_id = None
        elif host.isdigit():
            host_id = host
        else:
            host_id = hosts.uiShortnameToHostId(host)
        return reportdata.getLoadReportData(host_id)


