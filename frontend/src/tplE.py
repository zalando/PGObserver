import hosts
import json
import os
from jinja2 import Environment, FileSystemLoader

env = None
_settings = None


def setup(settings = None):
    global env, _settings
    hosts.resetHostsAndGroups()

    _settings = settings.get('features', {})

    if env is None:
        env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))

    env.globals['hosts'] = hosts.getHostData()
    env.globals['hosts_json'] = json.dumps(env.globals['hosts'])
    env.globals['settings'] = _settings
    hl = sorted(env.globals['hosts'].values(), key=lambda h: h['uishortname'])

    gs = {}
    hlf = []
    for h in hl:
        if h['host_group_id'] == None:
            hlf.append(h)
        else:
            if h['host_group_id'] in gs:
                continue
            else:
                gs[h['host_group_id']] = True
                hlf.append(h)

    env.globals['hostlist'] = hlf

    groups = {}

    for h in hosts.getHosts().values():
        if not (h['host_group_id'] in groups):
            groups[h['host_group_id']] = []

        groups[h['host_group_id']].append(h)

    for g in groups.keys(): # TODO remove?
        groups[g] = sorted(groups[g], key = lambda x : x['uishortname'])

    env.globals['hostgroups'] = groups
    env.globals['groups'] = hosts.getGroups()
    env.globals['groups_json'] = json.dumps(hosts.getGroups())