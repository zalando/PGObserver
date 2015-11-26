import cherrypy
import hosts
import tplE


class HostsFrontend(object):
    def __init__(self):
        self.show = self.index()

    def index(self, sortkey='host_ui_shortname'):
        allHosts = None
        show_hosts_page = tplE._settings.get('show_hosts_page', True)
        if show_hosts_page:
            allHosts = hosts.getAllHostsData()
            allHosts = sorted(allHosts.iteritems(), key=lambda h: h[1][sortkey])
        tmpl = tplE.env.get_template('hosts.html')
        return tmpl.render(all_hosts=allHosts, show_hosts_page=show_hosts_page,
                           target='World')

    def raw(self):
        return list(h[1] for h in sorted(hosts.getAllHostsData().iteritems()))

    def save(self, **params):
        try:
            hosts.saveHost(params)
        except Exception as e:
            return e.message
        if len(hosts.getHosts()) == 0:
            return 'Saved...NB! Dummy User/Password inserted, please correct in DB'
        return 'OK'

    def reload(self):
        tplE.setup({'features':tplE.env.globals['settings']})
        raise cherrypy.HTTPRedirect(cherrypy.url('/hosts'))

    index.exposed = True
    save.exposed = True
    reload.exposed = True
