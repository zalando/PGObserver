from multiprocessing import Process
import logging
import random
import time
import traceback

import pgobserver_gatherer.datastore_adapter as datastore_adapter
from pgobserver_gatherer import globalconfig
from pgobserver_gatherer.delta_engine import DeltaEngine
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.output_handlers.handler_base import HandlerBase
from pgobserver_gatherer.globalconfig import output_handlers


class GathererBase(Process):
    """Each gatherer is a separate process"""

    def __init__(self, host_data, settings, dataset_name):
        Process.__init__(self)
        self.gatherer_name = self.__class__.__name__
        self.host_data = host_data
        self.settings = settings
        self.host_id = host_data['host_id']
        self.host_name = host_data['host_ui_shortname'].lower().replace('-', '')
        self.interval_seconds = 300
        self.dataset_name = dataset_name
        self.datastore_table_name = None
        self.dataset_keys = []  # columns that define a disctinct object within a multirow dataset. e.g. schema, table_name for table IO data
        self.columns_to_store = None
        self.retry_queue = []   # in case of metrics storing fails, metrics will be kept in memory and retried on the next run
        self.first_run = True   # will be used to randomize 1st gathering to offload DB
        self.pg_server_version_num = None
        self.get_and_store_postgres_version()
        self.check_zz_utils()   # TODO move to workers, not everybody needs it
        self.delta_engine = None
        if globalconfig.config.get('features', {}).get('calculate_deltas', True):
            self.delta_engine = DeltaEngine(self.host_name, self.dataset_name, self.dataset_keys,
                                            globalconfig.config.get('features', {}).get('simple_deltas', False))

    def get_and_store_postgres_version(self):
        sql = """
            select current_setting('server_version_num')::int as server_version_num
        """
        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql, (self.host_id,))
        if data:
            self.pg_server_version_num = data[0]['server_version_num']
        else:
            logging.error('[%s][%s] determining Postgres version failed', self.gatherer_name, self.host_name)
        logging.debug('[%s][%s] pg_server_version_num = %s', self.gatherer_name, self.host_name, self.pg_server_version_num)
        return self.pg_server_version_num

    def check_zz_utils(self):
        sql = """
            select exists (select * from pg_proc p join pg_namespace n on p.pronamespace = n.oid where n.nspname = 'zz_utils' and p.proname = 'get_load_average') as have_zz_utils
        """
        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql, (self.host_id,))
        if data:
            self.have_zz_utils = data[0]['have_zz_utils']
        else:
            logging.error('[%s][%s] checking for zz_utils failed', self.gatherer_name, self.host_name)
        logging.debug('[%s][%s] have_zz_utils = %s', self.gatherer_name, self.host_name, self.pg_server_version_num)
        return self.have_zz_utils

    def gather_data(self):
        raise Exception('Only subclass implementation should be called!')

    def store_data(self, data, columns_to_store=None, dataset_name=None):
        logging.info('[%s][%s] running default store_data() for %s rows', self.host_name, self.gatherer_name, len(data))

        if not columns_to_store:
            columns_to_store = self.columns_to_store
        if not dataset_name:
            dataset_name = self.datastore_table_name

        datastore_adapter.store_to_postgres_metrics_db(data, dataset_name, columns_to_store)

    def process_retry_queue_if_any(self):
        if self.retry_queue and len(self.retry_queue) > 0:
            logging.info('[%s][%s] processing retry queue with %s datasets from', self.host_name, self.gatherer_name,
                         len(self.retry_queue))
            while len(self.retry_queue) > 0:
                old_data = self.retry_queue[0]
                self.store_data(old_data)
                self.retry_queue.pop(0)
                logging.info('[%s:%s] successfully stored 1 retry queue set', self.host_name, self.gatherer_name)

    def run(self):
        if self.first_run and not self.settings.get('testMode'):
            startup_sleep_seconds = random.random() / (4 if self.interval_seconds > 600 else 2) * self.interval_seconds
            startup_sleep_seconds = min(startup_sleep_seconds, 180)     # 3min max
            logging.info('[%s][%s] startup "random-sleeping" for %s s...', self.host_name, self.gatherer_name, round(startup_sleep_seconds, 1))
            time.sleep(startup_sleep_seconds)
            self.first_run = False

        while True:
            start_time = time.time()
            loop_start_time = start_time
            data = []
            try:

                logging.debug('[%s][%s] starting gather/store loop...', self.host_name, self.gatherer_name)

                # get data from remote host and push to remote pgobserver data service and/or store locally
                data = self.gather_data()
                duration = time.time() - start_time
                logging.info('[%s][%s] gather_data finished in %s s. rows=%s', self.host_name, self.gatherer_name,
                             round(duration, 2), len(data))
                # logging.info('[%s][%s] data: %s', self.host_name, self.gatherer_name, data)

                if globalconfig.config.get('database', {}).get('store_raw_metrics'):
                    self.process_retry_queue_if_any()

                if data:

                    if globalconfig.config.get('database', {}).get('store_raw_metrics'):
                        # logging.debug('data[0]: %s', data[0])
                        start_time = time.time()

                        self.store_data(data)

                        duration = time.time() - start_time
                        logging.info('[%s][%s] store_data finished in %s s', self.host_name, self.gatherer_name,
                                     round(duration, 2))

                    transformed = data
                    if self.delta_engine and len(output_handlers) > 0 :
                        # get delta and push to handler queues
                        try:
                            logging.debug('pre transform data[0]: %s', data[0])
                            transformed = self.delta_engine.add_snapshot_and_return_transformed(data)
                            logging.debug('transformed data[0]: %s', transformed[0])
                        except:
                            logging.exception('delta engine failure')

                    for handler_name, handler in globalconfig.output_handlers.items():
                        handler.queue.put((self.host_name, self.dataset_name, transformed))

            except KeyboardInterrupt:
                break

            except Exception as e:
                if data:
                    self.retry_queue.append(data)
                logging.error('[%s][%s] gather/store loop failed!: %s', self.host_name, self.gatherer_name,
                              traceback.format_exc())
            finally:
                loop_duration = time.time() - loop_start_time
                time.sleep(self.interval_seconds - loop_duration if loop_duration < self.interval_seconds else 0)


