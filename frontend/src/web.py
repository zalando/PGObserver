#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cherrypy
import os
import collections
import api
import monitorfrontend
import tablesfrontend
import sprocsfrontend
import indexesfrontend
import report
import performance
import hosts
import export
import hostsfrontend
import welcomefrontend
import datadb
import tplE
import yaml

from argparse import ArgumentParser

DEFAULT_CONF_FILE = '~/.pgobserver.yaml'


def main():
    parser = ArgumentParser(description='PGObserver Frontend')
    parser.add_argument('-c', '--config', help='Path to yaml config file with datastore connect details. Default location - {} \
        If not found then ENV vars PGOBS_HOST, PGOBS_DBNAME, PGOBS_USER, PGOBS_PASSWORD [, PGOBS_PORT]  will be used'.format(DEFAULT_CONF_FILE),
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('-p', '--port', help='Web server port. Overrides value from config file', dest='port', type=int)

    args = parser.parse_args()

    settings = collections.defaultdict(dict)

    if args.config:
        args.config = os.path.expanduser(args.config)

        if not os.path.exists(args.config):
            print 'WARNING. Config file {} not found! exiting...'.format(args.config)
            return
        print "trying to read config file from {}".format(args.config)
        with open(args.config, 'rb') as fd:
            settings = yaml.load(fd)

    # Make env vars overwrite yaml file, to run via docker without changing config file
    settings['database']['host'] = (os.getenv('PGOBS_HOST') or settings['database'].get('host'))
    settings['database']['port'] = (os.getenv('PGOBS_PORT') or settings['database'].get('port') or 5432)
    settings['database']['dbname'] = (os.getenv('PGOBS_DATABASE') or settings['database'].get('name'))
    settings['database']['user'] = (os.getenv('PGOBS_USER') or settings['database'].get('user'))
    settings['database']['password'] = (os.getenv('PGOBS_PASSWORD') or settings['database'].get('password'))

    if not (settings['database'].get('host') and settings['database'].get('dbname') and settings['database'].get('user') and settings['database'].get('password')):
        print 'Mandatory datastore connect details missing!'
        print 'Check --config input or environment variables: PGOBS_HOST, PGOBS_DATABASE, PGOBS_USER, PGOBS_PASSWORD [, PGOBS_PORT]'
        print ''
        parser.print_help()
        return

    conn_string = ' '.join((
        'dbname=' + settings['database']['dbname'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['user'],
        'port=' + str(settings['database']['port']),
    ))

    print 'Setting connection string to ... ' + conn_string

    conn_string = ' '.join((
        'dbname=' + settings['database']['dbname'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['user'],
        'password=' + settings['database']['password'],
        'port=' + str(settings['database']['port']),
    ))

    datadb.setConnectionString(conn_string)

    current_dir = os.path.dirname(os.path.abspath(__file__))

    conf = {'global':
                {
                    'server.socket_host': '0.0.0.0',
                    'server.socket_port': args.port or settings.get('frontend', {}).get('port') or 8080
                },
            '/':
                {
                    'tools.staticdir.root': current_dir
                },
            '/static':
                {
                    'tools.staticdir.dir': 'static',
                    'tools.staticdir.on': True
                },
            '/manifest.info':
                {
                    'tools.staticfile.on': True,
                    'tools.staticfile.filename': os.path.join(current_dir, '..', 'MANIFEST.MF'),
                    'tools.auth_basic.on': False
                }

            }

    tplE.setup(settings)    # setup of global variables and host data for usage in views

    root = welcomefrontend.WelcomeFrontend()

    for h in hosts.getHostData().values():
        mf = monitorfrontend.MonitorFrontend(h['host_id'])

        setattr(root, h['uishortname'], mf)
        setattr(root, str(h['host_id']), mf) # allowing host_id's for backwards comp

    root.report = report.Report()
    root.export = export.Export()
    root.perftables = performance.PerfTables()
    root.perfapi = performance.PerfApi()
    root.perfindexes = performance.PerfIndexes()
    root.perfschemas = performance.PerfUnusedSchemas()
    root.perflocks = performance.PerfLocksReport()
    root.perfstatstatements = performance.PerfStatStatementsReport()
    root.perfbloat = performance.PerfBloat()
    root.sprocs = sprocsfrontend.SprocFrontend()
    root.tables = tablesfrontend.TableFrontend()
    root.indexes = indexesfrontend.IndexesFrontend()
    root.hosts = hostsfrontend.HostsFrontend()
    root.api = api.Root(root)   # JSON api exposure, enabling integration with other monitoring tools

    if settings.get('oauth2',{}).get('redirect_url'):
        print 'switching on oauth2 ...'
        import zalandoauth
        root.zalandoauth = zalandoauth.ZalandOauth(settings['oauth2'])
        cherrypy.tools.zalandoauthtool = root.zalandoauth

    cherrypy.quickstart(root, config=conf)


if __name__ == '__main__':
    main()
