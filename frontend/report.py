from __future__ import print_function
'''
Created on Sep 20, 2011

@author: jmussler
'''
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

