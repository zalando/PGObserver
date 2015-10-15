#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from StringIO import StringIO
import boto

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


def get_s3_key_as_string(s3_url):
    """ Returns a file from S3 as string. Machine should have access to S3

    Args:
        s3_url (str): in form of 'https://s3-eu-west-1.amazonaws.com/x/y/file.yaml'
    Returns:
        str: content of given s3 bucket as string
    """
    m = re.match('https?://s3-(.*)\.amazonaws.com/(.*)', s3_url)
    if not m:
        raise Exception('Invalid S3 url: {}'.format(s3_url))
    region_name, bucket_path_with_key_name = m.groups()
    splits = bucket_path_with_key_name.split('/')
    conn = boto.s3.connect_to_region(region_name)
    bucket = conn.get_bucket(bucket_name=splits[0])
    if not bucket:
        raise Exception('S3 bucket not found: {}'.format(splits[0]))
    keys = list(bucket.list(prefix='/'.join(splits[1:])))
    if len(keys) != 1:
        raise Exception('S3 key not found: {}'.format('/'.join(splits[1:])))
    return keys[0].get_contents_as_string()


def get_config_as_dict_from_s3_file(s3_path):
    file_as_str = get_s3_key_as_string(s3_path)
    return yaml.load(StringIO(file_as_str))


class Healthcheck(object):
    def default(self, *args, **kwargs):
        return {}
    default.exposed = True


def main():
    parser = ArgumentParser(description='PGObserver Frontend')
    parser.add_argument('-c', '--config', help='Path to yaml config file with datastore connect details. See pgobserver_frontend.example.yaml for a sample file. \
        Certain values can be overridden by ENV vars PGOBS_HOST, PGOBS_DBNAME, PGOBS_USER, PGOBS_PASSWORD [, PGOBS_PORT]')
    parser.add_argument('--s3-config-path', help='Path style S3 URL to a key that holds the config file. Or PGOBS_CONFIG_S3_BUCKET env. var',
                              metavar='https://s3-region.amazonaws.com/x/y/file.yaml',
                              default=os.getenv('PGOBS_CONFIG_S3_BUCKET'))
    parser.add_argument('-p', '--port', help='Web server port. Overrides value from config file', type=int)

    args = parser.parse_args()

    settings = collections.defaultdict(dict)

    if args.s3_config_path:         # S3 has precedence if specified
        settings = get_config_as_dict_from_s3_file(args.s3_config_path)
    elif args.config:
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
    settings['database']['name'] = (os.getenv('PGOBS_DATABASE') or settings['database'].get('name'))
    settings['database']['frontend_user'] = (os.getenv('PGOBS_USER') or settings['database'].get('frontend_user'))
    settings['database']['password'] = (os.getenv('PGOBS_PASSWORD') or settings['database'].get('frontend_password'))

    if not (settings['database'].get('host') and settings['database'].get('name') and settings['database'].get('frontend_user')):
        print 'Mandatory datastore connect details missing!'
        print 'Check --config input or environment variables: PGOBS_HOST, PGOBS_DATABASE, PGOBS_USER, PGOBS_PASSWORD [, PGOBS_PORT]'
        print ''
        parser.print_help()
        return

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'port=' + str(settings['database']['port']),
    ))
    print 'Setting connection string to ... ' + conn_string
    # finished print conn_string to the world, password can be added
    conn_string = conn_string + ' password=' + settings['database']['frontend_password']

    datadb.setConnectionString(conn_string)

    current_dir = os.path.dirname(os.path.abspath(__file__))

    conf = {
        'global': {'server.socket_host': '0.0.0.0', 'server.socket_port': args.port or settings.get('frontend',
                   {}).get('port') or 8080},
        '/': {'tools.staticdir.root': current_dir},
        '/healthcheck': {'tools.sessions.on': False},
        '/static': {'tools.staticdir.dir': 'static', 'tools.staticdir.on': True, 'tools.sessions.on': False},
        '/manifest.info': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(current_dir, '..',
                           'MANIFEST.MF'), 'tools.auth_basic.on': False, 'tools.sessions.on': False},
    }

    tplE.setup(settings)  # setup of global variables and host data for usage in views

    root = welcomefrontend.WelcomeFrontend()

    for h in hosts.getHostData().values():
        mf = monitorfrontend.MonitorFrontend(h['host_id'])

        setattr(root, h['uishortname'], mf)
        setattr(root, str(h['host_id']), mf)  # allowing host_id's for backwards comp

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
    root.api = api.Root(root)  # JSON api exposure, enabling integration with other monitoring tools
    root.healthcheck = Healthcheck()

    if settings.get('oauth', {}).get('enable_oauth', False):
        print 'switching on oauth ...'
        import oauth
        root.oauth = oauth.Oauth(settings['oauth'])
        cherrypy.config.update({'tools.oauthtool.on': True, 'tools.sessions.on': True,
                                      'tools.sessions.timeout': settings['oauth'].get('session_timeout', 43200)})

    cherrypy.quickstart(root, config=conf)


if __name__ == '__main__':
    main()
