from __future__ import print_function
'''
@author: jmussler
'''
import logdata
import flotgraph
import time
from jinja2 import Environment, FileSystemLoader
import tplE

env = tplE.env

class LogfileFrontend(object):

        def __init__(self, hostId = 1):
            self.hostId = hostId

        def show(self):
            tmpl = env.get_template('logfiles.html')

            graphtemp = flotgraph.Graph("tempfilesgraph")
            graphtemp.addSeries('Temporary files','temp_files','#FF0000')
            tempfile_data = logdata.load_temporary_lines(self.hostId)

            for p in tempfile_data:
                graphtemp.addPoint('temp_files', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            grapherror = flotgraph.Graph("errorgraph")
            grapherror.addSeries('Errors','errors','#FF0000')
            grapherror.addSeries('User','users','#FF9900')
            error_data = logdata.load_error_lines(self.hostId)
            last_x = 0
            for p in error_data:
                if last_x != 0 and int(time.mktime(p[0].timetuple())) - last_x > 10*60:
                    grapherror.addPoint('errors', (last_x + 60)*1000, 0)
                    grapherror.addPoint('errors', (int(time.mktime(p[0].timetuple())) - 60)*1000, 0)

                grapherror.addPoint('errors', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            error_data = logdata.load_user_error_lines(self.hostId)

            last_x = 0
            for p in error_data:
                if last_x != 0 and int(time.mktime(p[0].timetuple())) - last_x > 10*60:
                    grapherror.addPoint('users', (last_x + 60)*1000, 0)
                    grapherror.addPoint('users', (int(time.mktime(p[0].timetuple())) - 60)*1000, 0)
                grapherror.addPoint('users', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            graphtimeout = flotgraph.Graph("timeoutgraph")
            graphtimeout.addSeries('Timeouts','timeout','#FF0000')
            graphtimeout.addSeries('User','users','#FF9900')
            timeout_data = logdata.load_timeout_lines(self.hostId)

            last_x = 0
            for p in timeout_data:
                if last_x != 0 and int(time.mktime(p[0].timetuple())) - last_x > 10*60:
                    graphtimeout.addPoint('timeout', (last_x + 60)*1000, 0)
                    graphtimeout.addPoint('timeout', (int(time.mktime(p[0].timetuple())) - 60)*1000, 0)

                last_x = int(time.mktime(p[0].timetuple()))

                graphtimeout.addPoint('timeout', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            timeout_data = logdata.load_user_timeout_lines(self.hostId)
            for p in timeout_data:
                graphtimeout.addPoint('users', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            graphwait = flotgraph.Graph("waitgraph")
            graphwait.addSeries('Waits','waits','#FF0000')
            wait_data = logdata.load_wait_lines(self.hostId)

            for p in wait_data:
                graphwait.addPoint('waits', int(time.mktime(p[0].timetuple()) * 1000) , p[1])

            return tmpl.render(hostid=self.hostId,
                               tempfilesgraph = graphtemp.render(),
                               errorgraph = grapherror.render(),
                               timeoutgraph = graphtimeout.render(),
                               waitgraph = graphwait.render(),
                               target='World')

        show.exposed = True

