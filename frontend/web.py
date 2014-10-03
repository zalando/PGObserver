#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cherrypy
import os.path
import json

import MonitorFrontend
import tablesfrontend
import sprocsfrontend
import logdata
import report
import performance
import hosts
import export

import DataDB
import tplE
import yaml

from argparse import ArgumentParser

DEFAULT_CONF_FILE = '~/.pgobserver.conf'

def main():
    parser = ArgumentParser(description='PGObserver Frontend')
    parser.add_argument('-c', '--config', help='Path to config file. (default: %s)' % DEFAULT_CONF_FILE, dest='config',
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('-p', '--port', help='server port', dest='port', type=int)

    args = parser.parse_args()

    args.config = os.path.expanduser(args.config)

    yaml_file = args.config.replace(".conf", ".yaml")

    settings = None
    if os.path.exists(yaml_file):
        print "trying to read config file from {}".format(yaml_file)
        with open(yaml_file, 'rb') as fd:
            settings = yaml.load(fd)

    if settings is None and os.path.exists(args.config):
        print "trying to read config file from {}".format(os.path.exists(args.config))
        with open(args.config, 'rb') as fd:
            settings = json.load(fd)

    if settings is None:
        print 'Config file missing, neither Yaml nor JSON file could be found'
        parser.print_help()
        return

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'port=' + str(settings['database']['port']),
    ))

    print 'Setting connection string to ... ' + conn_string

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'password=' + settings['database']['frontend_password'],
        'port=' + str(settings['database']['port']),
    ))

    DataDB.setConnectionString(conn_string)

    if 'logfiles' in settings:
        logdata.setFilter(settings['logfiles']['liveuserfilter'])

    current_dir = os.path.dirname(os.path.abspath(__file__))

    conf = {'global': {'server.socket_host': '0.0.0.0', 'server.socket_port': args.port or settings.get('frontend',
            {}).get('port') or 8080}, '/': {'tools.staticdir.root': current_dir},
            '/static': {'tools.staticdir.dir': 'static', 'tools.staticdir.on': True}}

    tplE.setup(settings)

    root = None

    for h in hosts.getHostData().values():
        mf = MonitorFrontend.MonitorFrontend(h['host_id'])

        if root == None:
            root = mf

        setattr(root, h['uishortname'], mf)

    root.report = report.Report()
    root.export = export.Export()
    root.perftables = performance.PerfTables()
    root.perfapi = performance.PerfApi()
    root.perfindexes = performance.PerfIndexes()
    root.sprocs = sprocsfrontend.SprocFrontend()
    root.tables = tablesfrontend.TableFrontend()

    cherrypy.quickstart(root, config=conf)


if __name__ == '__main__':
    main()
