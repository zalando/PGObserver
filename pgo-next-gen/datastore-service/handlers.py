import datadb
import logging
from datetime import datetime
from datetime import timedelta
from psycopg2.extensions import adapt
import pgo_helpers


def get_hosts(host_id=None):
    sql = """select
               host_id,
               host_name,
               host_port,
               host_db,
               host_settings,
               host_enabled,
               host_gather_group,
               host_created::text,
               host_last_modified::text,
               host_ui_shortname,
               host_ui_longname
            from hosts where %(host_id)s is null or host_id = %(host_id)s"""
    data = datadb.execute(sql, {'host_id': host_id})
    return data

def get_all_hosts():
    return get_hosts()


def get_load(host_ids, from_date, to_date):
    sql = """select
               load_host_id as host_id,
               load_timestamp::text as tz,
               extract(epoch from load_timestamp) as tz_epoch,
               load_1min_value as "1min",
               load_5min_value as "5min",
               load_15min_value as "15min",
               xlog_location_mb
             from
               host_load
             where
               load_host_id = any(%(host_ids)s)
               and load_timestamp >= %(from_date)s
               and load_timestamp < %(to_date)s
             order by
               load_host_id
           """
    # print(datadb.mogrify(sql, {'host_ids': host_ids, 'from_date': from_date, 'to_date': to_date}))
    data = datadb.execute(sql, {'host_ids': host_ids, 'from_date': from_date, 'to_date': to_date})
    return data


def get_sprocs(host_id, from_date=None):
    sql = """select
               sproc_id,
               sproc_schema,
               sproc_name
            from
              sprocs
            where
              sproc_host_id = %(host_id)s
              and (%(from_date)s is null or sproc_created >= %(from_date)s)
            order by
              2, 3
          """
    data = datadb.execute(sql, {'host_id': host_id, 'from_date': from_date})
    return data


def get_sproc_perf_by_id(host_id, sproc_id, from_date, to_date):
    sql = """select
               sp_host_id as host_id,
               sp_timestamp::text as tz,
               extract(epoch from sp_timestamp) as tz_epoch,
               sp_calls as calls,
               sp_total_time as total,
               sp_self_time as self
             from
               sproc_performance_data
             where
               sp_host_id = %(host_id)s
               and sp_sproc_id = %(sproc_id)s
               and sp_timestamp >= %(from_date)s
               and sp_timestamp < %(to_date)s
             order by
               sp_host_id, sp_timestamp
           """
    return datadb.execute(sql, {'host_id': host_id, 'sproc_id': sproc_id, 'from_date': from_date, 'to_date': to_date})


def get_sproc_perf_by_name(host_ids, sproc_name, from_date, to_date):
    sql = """select
               sp_host_id as host_id,
               sp_timestamp::text as tz,
               extract(epoch from sp_timestamp) as tz_epoch,
               sp_calls as calls,
               sp_total_time as total,
               sp_self_time as self
             from
               sproc_performance_data
             where
               sp_host_id = {host_id}
               and sp_sproc_id in (
                 select sproc_id from sprocs where sproc_host_id = {host_id} and sproc_name like '{sproc_name}%'
                 )
               and sp_timestamp >= %(from_date)s
               and sp_timestamp < %(to_date)s
             order by
               sp_host_id, sp_timestamp
           """
    sql = ''
    for id in host_ids:
        if sql:
            sql += '\nunion all'
        sql += sql.format({'host_id': id, 'sproc_name': sproc_name})
    return datadb.execute(sql, {'from_date': from_date, 'to_date': to_date})


def get_stat_database(host_ids, from_date, to_date):
    sql = """select
               sdd_host_id as host_id,
               sdd_timestamp::text as tz,
               extract(epoch from sdd_timestamp) as tz_epoch,
               sdd_numbackends as numbackends,
               sdd_xact_commit as commits,
               sdd_xact_rollback as rollbacks,
               sdd_blks_read as blks_read,
               sdd_blks_hit as blks_hit,
               sdd_temp_files as temp_files,
               sdd_temp_bytes as temp_bytes,
               sdd_deadlocks as deadlocks,
               sdd_blk_read_time as blk_read_time,
               sdd_blk_write_time as blk_write_time
             from
               stat_database_data
             where
               (%(host_ids)s = '{}' or sdd_host_id = any(%(host_ids)s))
               and sdd_timestamp >= %(from_date)s
               and sdd_timestamp < %(to_date)s
           """
    return datadb.execute(sql, {'host_ids': host_ids, 'from_date': from_date, 'to_date': to_date})


def get_data_statements(host_ids, from_date, to_date, order_by='calls', limit='50'):
    sql = """select
               *
             from
               stat_statements_data
             where
               (%(host_ids)s = '{}' or ssd_host_id = any(%(host_ids)s)
               and ssd_timestamp >= %(from_date)s
               and ssd_timestamp < %(to_date)s
              order by
                {order_by} desc
              limit {limit}
           """.format(order_by=order_by, limit=limit)
    data = datadb.execute(sql, {'host_ids': host_ids, 'from_date': from_date, 'to_date': to_date})
    return data


def create_publishing_host(params):
    logging.debug('create_publishing_host(params=%s)', params)

    # check if host exists
    sql_check = """select * from hosts where host_ui_shortname = %(host_ui_shortname)s"""
    data = datadb.execute(sql_check, params)
    if data:
        raise Exception('Host already existing: ' + params['host_ui_shortname'])

    # check if host_group_exists, if not create
    sql_get_group_id = """select group_id from host_groups where group_name = %(host_gather_group)s"""
    data = datadb.execute(sql_get_group_id, params)
    if data:
        params['host_group_id'] = data[0]['group_id']
    else:
        sql_create_group = """insert into host_groups (group_name)
                                select %(host_gather_group)s where not exists (select 1 from host_groups where group_name = %(host_gather_group)s)
                                returning group_id"""
        data = datadb.execute(sql_create_group, params)
        print(data)
        params['host_group_id'] = data[0]['group_id']

    sql_create_host = """insert into hosts (host_name, host_db, host_ui_shortname, host_ui_longname, host_gather_group, host_group_id)
                            select %(host_name)s, %(host_db)s, %(host_ui_shortname)s, %(host_ui_longname)s, %(host_gather_group)s, %(host_group_id)s
                            returning *"""
    data = datadb.execute(sql_create_host, params)
    # print(data)
    return {'host_id': data[0]['host_id']}


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    # data = [(1, '2015-06-18 11:44:30.211411', 23), (1, '2015-06-18 11:45:30.211411', 26)]
    # print(dataset_to_delimited_stringio(data).getvalue())
    # print(store_dataset_to_table('sprocs', 1, [(1,'bas')], ['sproc_host_id', 'sproc_schema'], []))
    # print(store_dataset_to_table('sprocs', 1, [(1,'bas'), (1,'bar')], ['sproc_host_id', 'sproc_schema'], ['sproc_id']))

    # post_data = {'host_name': 'host_name', 'host_db': 'host_db', 'host_ui_shortname': 'host_ui_shortname',
    #              'host_ui_longname': 'host_ui_longname', 'host_gather_group': 'host_gather_group'}
    # print(create_publishing_host(post_data))

    print(get_data_sproc_perf([1],'2015-06-01', '2015-06-02', 6))
    print(get_data_sproc_perf([1],'2015-06-01', '2015-06-02', sproc_name=''))