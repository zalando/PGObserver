import logging
import re
import time
from collections import OrderedDict
from pgobserver_gatherer.output_handlers.handler_base import HandlerBase
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


def humanize(val):
    return 'humanized ' + str(val)  # TODO


def pretty_size(size):
    """ mimics pg_size_pretty() """
    if size is None:
        return None
    sign = '-' if size < 0 else ''
    size = abs(size)
    if size <= 1024:
        return sign + str(size) + ' B'
    if size < 10 * 1024**2:
        return sign + str(round(size / 1024)) + ' kB'
    if size < 10 * 1024**3:
        return sign + str(round(size / 1024**2)) + ' MB'
    if size < 10 * 1024**4:
        return sign + str(round(size / 1024**3)) + ' GB'
    return sign + str(round(size / 1024**4)) + ' TB'


class Transformations:
    TF_TIME_NICE = lambda x: x.time().replace(microsecond=0)
    ROUND = lambda x: round(x) if x is not None else x
    TF_HUMAN_READABLE = lambda x: humanize(x) if x is not None else x
    SIZE_PRETTY = pretty_size


def apply_transformations(data_val, transformation_list):
    if not transformation_list:
        return data_val
    logging.debug('[consolehandler] applying transformations to "%s"', data_val)

    data_ret = data_val
    for tf_lambda in transformation_list:
        data_ret = tf_lambda(data_ret)
    return data_ret


class Handler(HandlerBase):
    DATASET_CONFIG = {
            Datasets.BGWRITER: OrderedDict([
                ('sbd_timestamp', {}),
                ('sbd_checkpoints_timed', {}),
                ('sbd_checkpoints_req', {}),
                ('sbd_checkpoint_write_time', {}),
                ('sbd_buffers_checkpoint', {}),
                ('sbd_buffers_backend', {}),
                ('sbd_buffers_alloc', {}),
            ]),
            Datasets.DB: OrderedDict([
                ('sdd_timestamp', {'transform': [Transformations.TF_TIME_NICE]}),
                ('sdd_numbackends', {}),
                ('sdd_temp_bytes', {'transform': [Transformations.ROUND], 'unit': 'times_min'}),
                ('sdd_temp_files', {'transform': [Transformations.ROUND]}),
                ('sdd_blks_read', {'transform': [Transformations.ROUND]}),
                ('sdd_blks_hit', {'transform': [Transformations.ROUND]}),
                ('cache_hit_pct', {'transform': [Transformations.ROUND]}),
                ('sdd_blk_read_time', {'transform': []}),
                ('sdd_blk_write_time', {'transform': []}),
            ]),
            Datasets.INDEX: OrderedDict([
                ('iud_timestamp', {}),
                ('i_schema', {}),
                # ('i_table_name', {}),
                ('i_name', {}),
                ('iud_scan', {}),
                ('iud_tup_read', {}),
                ('iud_tup_fetch', {}),
                ('iud_size', {'transform': [Transformations.SIZE_PRETTY]}),
            ]),
            Datasets.KPI: OrderedDict([
                ('kpi_timestamp', {}),
                ('kpi_load_1min', {}),
                ('kpi_active_backends', {}),
                ('kpi_blocked_backends', {}),
                ('kpi_tps', {'transform': []}),
                ('kpi_rollbacks', {'transform': []}),
                ('kpi_temp_bytes', {'transform': [Transformations.SIZE_PRETTY]}),
                ('kpi_wal_location_b', {'transform': [Transformations.SIZE_PRETTY]}),
                ('kpi_ins', {}),
                ('kpi_upd', {}),
                ('kpi_del', {}),
                ('kpi_sproc_calls', {}),
            ]),
            Datasets.LOAD: OrderedDict([
                ('load_timestamp', {}),
                ('load_1min_value', {}),
                ('xlog_location_b', {'transform': [Transformations.ROUND]}),
            ]),
            Datasets.SCHEMAS: OrderedDict([
                ('sud_timestamp', {}),
                ('sud_schema_name', {}),
                ('sud_seq_scans', {}),
                ('sud_idx_scans', {}),
                ('sud_sproc_calls', {}),
                ('sud_tup_ins', {}),
                ('sud_tup_upd', {}),
                ('sud_tup_del', {}),
            ]),
            Datasets.SPROCS: OrderedDict([
                ('sp_timestamp', {'transform': [Transformations.TF_TIME_NICE]}),
                ('schema_name', {}),
                ('function_name', {}),
                ('sp_calls', {'transform': [Transformations.ROUND], 'unit': 'times_min'}),
                # ('sp_self_time', {'transform': [Transformations.TF_ROUND]}),
                ('sp_total_time', {'transform': [Transformations.ROUND]}),
                ('sp_avg_runtime_s', {'transform': [Transformations.ROUND]}),
            ]),
            Datasets.TABLE: OrderedDict([
                ('tsd_timestamp', {}),
                ('t_schema', {}),
                ('t_name', {}),
                ('tsd_table_size', {}),
                ('tsd_index_size', {}),
                ('tsd_seq_scans', {}),
                ('tsd_index_scans', {}),
                # ('tsd_tup_ins', {}),
                # ('tsd_tup_upd', {}),
                # ('tsd_tup_hot_upd', {}),
                # ('tsd_tup_del', {}),
            ]),

        }

    def __init__(self, settings=None, filters=None):
        self.name = 'consolehandler'
        super().__init__(self.name, settings, filters)
        logging.info('[%s] initialized. settings: %s, filters: %s', self.name, settings, filters)
        self.rows_printed = 0

    def run(self):
        logging.debug('[%s] starting up ...', self.name)

        while True:
            try:
                if self.queue.qsize() > 0:
                    host_name, dataset_name, data = self.queue.get()
                    # if dataset_name not in self.DATASET_CONFIG:
                    #     continue
                    logging.debug('[%s] printing "%s" dataset', self.name, dataset_name)
                    self.print_item(dataset_name, data)
                else:
                    logging.debug('[%s] sleeping 1s ...', self.name)
                    time.sleep(1)

            except KeyboardInterrupt:
                break

    def print_item(self, dataset_name, dataset):
        logging.debug('[%s] print_item() - items: %s', self.name, len(dataset))
        tz_col = None

        if Handler.DATASET_CONFIG.get(dataset_name):
            cols_to_print = list(self.DATASET_CONFIG[dataset_name].keys())
        else:
            cols_to_print = sorted(dataset[0].keys())

        # moving *_timestamp to 1st position
        tz_col = SUPPORTED_DATASETS[dataset_name][1] + 'timestamp'
        if tz_col in cols_to_print:
            cols_to_print.pop(cols_to_print.index(tz_col))
            cols_to_print = [tz_col] + cols_to_print
        else:
            logging.warning('[%s][%s] timestamp column "%s" not found', self.name, dataset_name, )

        # removing *_host_id
        try:
            cols_to_print.remove(SUPPORTED_DATASETS[dataset_name][1] + 'host_id')
        except ValueError:
            pass
        cols_to_print_trimmed = []  # removing table prefix for printing the header
        for x in cols_to_print:
            cols_to_print_trimmed.append(re.sub(r'([a-z]+_)', '', x, 1))
        logging.debug('[%s] cols_to_print: %s', self.name, cols_to_print_trimmed)

        if self.rows_printed % 10 == 0:      # re-printing header every 10 lines
            print('\t'.join(cols_to_print_trimmed))
            # print('-'*60)

        for data in dataset:
            logging.debug('[%s] printing: %s', self.name, data)
            ds = []
            if self.filters and len(SUPPORTED_DATASETS[dataset_name]) > 2:  # TODO move to gatherer ?
                key_cols = SUPPORTED_DATASETS[dataset_name][2]
                logging.debug('[%s] start filtering - filters: %s, key_cols: %s', self.name, self.filters, key_cols)
                is_match = True
                for i, f in enumerate(self.filters):
                    if not data.get(key_cols[i]) or str(data[key_cols[i]]).find(f) == -1:
                        is_match = False
                        break
                if not is_match:
                    # logging.debug('[%s] row filtered out due to filter mismatch: %s', self.name, data)
                    continue
            for col in cols_to_print:
                if col not in data:
                    ds.append('NULL')
                    continue
                if col == tz_col:
                    col_val = apply_transformations(data[col], [Transformations.TF_TIME_NICE])
                elif self.DATASET_CONFIG.get(dataset_name, {}).get(col, {}).get('transform'):
                    col_val = apply_transformations(data[col],
                                                    self.DATASET_CONFIG.get(dataset_name, {}).get(col, {}).get('transform', []))
                else:
                    col_val = data[col]
                ds.append(str(col_val))

            print('\t'.join(ds))
            self.rows_printed += 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    h = Handler()
    h.start()
    print('handler started')
    print(h)
    print('q', h.queue)
    for i in range(1, 5):
        item = {'k1': i, 'k2': i*2}
        print('putting', item)
        h.queue.put(('set1', item))
        time.sleep(1)
