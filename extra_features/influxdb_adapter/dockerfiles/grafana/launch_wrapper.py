#!/usr/bin/python3

from argparse import ArgumentParser
#from configparser import ConfigParser
import os
import subprocess

GRAFANA_BIN = 'bin/grafana-server'
GRAFANA_CONF_SAMPLE = 'conf/sample.ini'
GRAFANA_CONF_CUSTOM = 'conf/custom.ini'

args = None


def replace_db_settings_and_write_to_custom_ini_file(grafana_folder):
    """ changing the DB section and rewriting it into another. counting on below ini format
...
# [database]
type = postgres
host = 127.0.0.1:5432
name = grafana
user = grafana
password = grafana

#################################### Session ####################################
[session]
...
    """
    # could use ConfigParser after this INI fix - https://github.com/grafana/grafana/issues/2774
    # config = ConfigParser()
    # config.read_file(open(os.path.join(grafana_folder, CONFDB_CONF)))
    # print(config.sections())
    ini_lines_in = open(os.path.join(grafana_folder, GRAFANA_CONF_SAMPLE)).read().splitlines()
    ini_lines_out = []

    db_section_index_start = ini_lines_in.index('[database]')
    db_section_index_end = ini_lines_in.index('[session]')

    # print(ini_lines_in[db_section_index_start+1:db_section_index_end-1])
    ini_lines_out.extend(ini_lines_in[0:db_section_index_start+1])

    ini_lines_out.append('type = {}'.format(args.provider))
    ini_lines_out.append('host = {}:{}'.format(args.host, args.port))
    ini_lines_out.append('name = {}'.format(args.dbname))
    ini_lines_out.append('user = {}'.format(args.user))
    ini_lines_out.append('password = {}'.format(args.password))

    ini_lines_out.extend(ini_lines_in[db_section_index_end-2:])

    print('\n'.join(ini_lines_out), file=open(os.path.join(grafana_folder, GRAFANA_CONF_CUSTOM), 'w'))


def main():
    parser = ArgumentParser(description='Making possible to provide Grafana "config DB" connection strings (postgres or mysql) on runtime')
    parser.add_argument('--grafana-folder', help='Full path to Grafana folder [default /grafana]', default='/grafana')
    parser.add_argument('--provider', help='[postgres (default) | mysql]. $CONFDB_PROVIDER env. var.', default=(os.getenv('CONFDB_PROVIDER') or 'postgres'))
    parser.add_argument('--host', help='DB host. $CONFDB_HOST env. var.', default=os.getenv('CONFDB_HOST'))
    parser.add_argument('--port', help='DB port. defaults to 5432. $CONFDB_PORT env. var.]', default=(os.getenv('CONFDB_PORT') or '5432'))
    parser.add_argument('--dbname', help='DB name. $CONFDB_DBNAME env. var.', default=os.getenv('CONFDB_DBNAME'))
    parser.add_argument('--user', help='DB user. Should have r/w rights on the DB. $CONFDB_USER env. var.', default=os.getenv('CONFDB_USER'))
    parser.add_argument('--password', help='DB pass. $CONFDB_PASSWORD env. var.', default=os.getenv('CONFDB_PASSWORD'))

    global args
    args = parser.parse_args()
    print(args)

    if not (args.host and args.dbname and args.user and args.password):
        print('Incomplete arguments!')
        parser.print_help()
        return

    replace_db_settings_and_write_to_custom_ini_file(args.grafana_folder)

    grafana_bin = os.path.join(args.grafana_folder, GRAFANA_BIN)
    grafana_conf = os.path.join(args.grafana_folder, GRAFANA_CONF_CUSTOM)
    print('launching: {} -config={} -homepath={} ...'.format(grafana_bin, grafana_conf, args.grafana_folder))
    subprocess.call([grafana_bin,
                    '-config={}'.format(grafana_conf),
                    '-homepath={}'.format(args.grafana_folder),
                     ])


if __name__ == '__main__':
    main()
