import yaml
import collections


class Datasets:
    BGWRITER = 'bgwriter'
    CONNS = 'conn'
    DB = 'db'
    INDEX = 'index'
    KPI = 'kpi'
    LOAD = 'load'
    LOCKS = 'locks'     # TODO console fails
    SCHEMAS = 'schema'
    SPROCS = 'sproc'    # TODO console fails
    STATEMENTS = 'statements'
    TABLE = 'table'
    TABLEIO = 'tableio'

SUPPORTED_DATASETS = {
        Datasets.BGWRITER: ('statBgwriterGatherInterval', 'sbd_'),     # (intervalSettingKey, prefix for returned data, (key_col1,...))
        Datasets.CONNS: ('statConnectionGatherInterval', 'scd_'),
        Datasets.DB: ('statDatabaseGatherInterval', 'sdd_'),
        Datasets.INDEX: ('indexStatsGatherInterval', 'iud_', ('i_schema', 'i_name')),
        Datasets.KPI: ('KPIGatherInterval', 'kpi_'),
        Datasets.LOAD: ('loadGatherInterval', 'load_'),
        Datasets.LOCKS: ('blockingStatsGatherInterval', 'bp_'),
        Datasets.SCHEMAS: ('schemaStatsGatherInterval', 'sud_', ('sud_schema_name',)),
        Datasets.SPROCS: ('sprocGatherInterval', 'sp_', ('schema_name', 'function_name')),
        Datasets.STATEMENTS: ('statStatementsGatherInterval', 'ssd_'),
        Datasets.TABLE: ('tableStatsGatherInterval', 'tsd_', ('t_schema', 't_name')),
        Datasets.TABLEIO: ('tableIoGatherInterval', 'tio_'),
}

config = collections.defaultdict(dict)
output_handlers = {}

ALL_GATHERER_INTERVAL_KEYS = [x[0] for x in SUPPORTED_DATASETS.values()]
ADDITIONAL_GATHERER_SETTING_KEYS = ['useTableSizeApproximation', 'isAWS', 'testMode']

SAMPLE_CONFIG_FILE_CONTENTS = """
---

database:
  store_raw_metrics: false   # should raw metrics data be stored to Postgres or only passed to plugins

  # PGObserver database
  name: local_pgobserver_db
  host: localhost
  port: 5432

  pool_size: 10   # max simultaneous connections

  # gatherer credentials for PGObserver database
  backend_user: pgobserver_gatherer
  backend_password: pgobserver_gatherer

  # configure which hosts to monitor ( relates to monitor_data.hosts.host_gather_group column)
  # needed for cases where splitting of hosts between daemons is needed due to workload or network
  gather_group: gatherer1


# Available metrics:
#- kpi
#- load
#- db
#- conns
#- sprocs
#- statements
#- table
#- tableio
#- bgwriter
#- indexes
#- locks

features:
  calculate_deltas: true    # if false, exactly what is returned from gatherers is sent to output plugins
  simple_deltas: false      # by default "change rates" are calculated i.e. deltas divided by passed time.
                            # when true (default for "console mode") then just pure delta values are passed to output
# FYI
# - even when some metrics are enabled here they might not be be actually enabled on single host level (hosts.host_settings)
# - plugins can be declared multiple times with different parameters
# - if "metrics" node is defined, only listed metrics data will be processed by this plugin
output_plugins:
  file:
    enabled: true
#    metrics:  # TODO implement filtering
#      load: true
  influxdb:
    enabled: false

"""


def read_config_from_yaml(file_path):
    global config

    with open(file_path, 'rb') as fd:
        config = yaml.load(fd)


def set_config_values(section_name, key_val_dict):
    global config
    config[section_name].update(key_val_dict)


def write_a_sample_config_file(file_path):
    with open(file_path, 'w') as f:
        print(SAMPLE_CONFIG_FILE_CONTENTS, file=f)
