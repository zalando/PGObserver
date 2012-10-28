'''
Created on Feb 8, 2012

@author: jmussler
'''

import hosts

from jinja2 import Environment, FileSystemLoader

env = None
_settings = None

def setup(settings = None):
    global env, _settings
    
    if env != None:
        return

    if settings != None:
        _settings = {"tags":False,"sizeOverview":False,"logfiles":False}
    
    env = Environment(loader=FileSystemLoader('templates'))    

    env.globals['hosts'] = hosts.getHostData()
    env.globals['settings'] = _settings;
    hl = sorted( env.globals['hosts'].values() , key = lambda h : h['settings']['uiShortName'] )
    
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
        if h['host_group_id'] > 0:
            if not (h['host_group_id'] in groups):                    
                groups[h['host_group_id']] = []
            
            groups[h['host_group_id']].append(h);
    
    for g in groups.keys():
        groups[g] = sorted(groups[g], key = lambda x : x['settings']['uiShortName'])
        
    env.globals['hostgroups'] = groups
    env.globals['groups'] = hosts.getGroups()