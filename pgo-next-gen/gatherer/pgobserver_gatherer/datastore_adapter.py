import logging
from io import StringIO

from psycopg2.extensions import adapt
import pgobserver_gatherer.datadb as datadb


def split_dict_data_to_tuples_for_given_columns(data_as_dicts: list, columns: list):
    """ preserves column order """
    data = []

    for d in data_as_dicts:
        temp = []
        for c in columns:
            if c not in d:
                raise Exception('invalid input! key={} not existing in data: {}'.format(c, d))
            temp.append(d[c])
        data.append(tuple(temp))

    return data


def generate_insert_with_returning(data, table_name, insert_column_names, return_column_names):
        sql = StringIO()
        sql.write('''INSERT INTO {} ('''.format(table_name))
        sql.write(', '.join(insert_column_names))
        sql.write(') VALUES ')
        for i, d in enumerate(data):
            if i > 0:
                sql.write(', ')
            for j, col_name in enumerate(insert_column_names):
                adapted = str(adapt(d[col_name]))
                if j == 0:
                    sql.write('(')
                else:
                    sql.write(', ')
                sql.write(adapted)
            sql.write(')')
        sql.write(' RETURNING ')
        sql.write(', '.join(return_column_names))
        sql_val = sql.getvalue()
        return sql_val


def store_to_postgres_metrics_db(data, table_name, insert_column_names, return_column_names=None):
    added_rows = []

    if return_column_names:
        sql_val = generate_insert_with_returning(data, table_name, insert_column_names, return_column_names)
        # logging.debug('store_to_postgres_metrics_db(): sql=%s', sql_val)
        return datadb.execute(sql_val)
    else:
        data = split_dict_data_to_tuples_for_given_columns(data, insert_column_names)
        data_in_copy_format_stringio = dataset_to_delimited_stringio(data)
        return datadb.copy_from(data_in_copy_format_stringio, table_name, insert_column_names)


def check_all_hosts_for_connectivity(hosts_data: list):
    err_count = 0
    for hd in hosts_data:
        logging.debug('checking host_id %s (%s:%s/%s) for DB connectivity...', hd['host_id'], hd['host_name'],
                      hd['host_port'], hd['host_db'])
        try:
            datadb.executeOnHost(hd['host_name'], hd['host_port'], hd['host_db'], hd['host_user'], hd['host_password'],
                                 'select * from pg_stat_database limit 1')
        except Exception as e:
            logging.error('failed to execute test select on host_id %s: %s', hd['host_id'], e)
            err_count += 1
    return err_count


def dataset_to_delimited_stringio(data, delimiter='\t'):
    str_io = StringIO()
    delimiter_escaped = '\\' + delimiter

    for row in data:
        row_as_str = [(str(x).replace(delimiter, delimiter_escaped) if x is not None else r'\N') for x in row]
        csv_row = delimiter.join(row_as_str)
        str_io.write(csv_row)
        str_io.write('\n')

    logging.debug('str_io.getvalue: %s', str_io.getvalue())
    str_io.seek(0)
    return str_io


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='(%(levelname)s) %(message)s)')

    print(generate_insert_with_returning(data=[{'c1': 't1', 'c2': 't2'}, {'c1': 't11', 'c2': 't22'}],
                                         table_name='mytbl', insert_column_names=['c1', 'c2'], return_column_names=['id']))

    exit()
    datadb.init_connection_pool()
    datadb.execute('create table if not exists pgotest1 (c1 text, c2 int)')
    # datadb.execute('drop table if exists pgotest1')

    data = [{'c1': 'a\tb', 'c2': 1}, {'c1': 'b', 'c2': 2}]
    print(store_to_postgres_metrics_db(data, 'pgotest1', ['c1', 'c2']))


