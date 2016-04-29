import re

from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


ROW_LIMITS_FOR_SINGLE_ORDERING_CRITERIONS = 500
QUERY_TRANSFORMATION_REGEXES = [
        {'pattern': re.compile(r'[\t\s]+'), 'replace': ' '},    # remove tabs + extra whitespace
        {'pattern': re.compile(r'/\*(.*?)\*/'), 'replace': ''}]     # remove multiline comments


class StatStatementsGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.STATEMENTS)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.STATEMENTS][0]] * 60
        self.columns_to_store = ['ssd_host_id', 'ssd_timestamp', 'ssd_query', 'ssd_query_id', 'ssd_calls', 'ssd_total_time',
                            'ssd_blks_read', 'ssd_blks_written', 'ssd_temp_blks_read', 'ssd_temp_blks_written']
        self.datastore_table_name = 'monitor_data.stat_statements_data'

    def gather_data(self):
        sql_get = '''
        with q_data as (
          select
            now() as ssd_timestamp,
            ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g')) as ssd_query,
            sum(s.calls)::int8 as ssd_calls,
            round(sum(s.total_time))::int8 as ssd_total_time,
            sum(shared_blks_read+local_blks_read)::int8 as ssd_blks_read,
            sum(shared_blks_written+local_blks_written)::int8 as ssd_blks_written,
            sum(temp_blks_read)::int8 as ssd_temp_blks_read,
            sum(temp_blks_written)::int8 as ssd_temp_blks_written
          from
            zz_utils.get_stat_statements() s
          where
            calls > 1
            and total_time > 0
            and not upper(s.query) like any (array['DEALLOCATE%', 'SET %', 'RESET %', 'BEGIN%', 'BEGIN;',
              'COMMIT%', 'END%', 'ROLLBACK%', 'SHOW%', '<INSUFFICIENT PRIVILEGE>'])
          group by
            query
        )
        select * from (
          select
            *
          from
            q_data
          where
            ssd_total_time > 0
          order by
            ssd_total_time desc
          limit {row_limit}
        ) a
        union
        select * from (
          select
            *
          from
            q_data
          order by
            ssd_calls desc
          limit {row_limit}
        ) a
        union
        select * from (
          select
            *
          from
            q_data
          where
            ssd_blks_read > 0
          order by
            ssd_blks_read desc
          limit {row_limit}
        ) a
        union
        select * from (
          select
            *
          from
            q_data
          where
            ssd_blks_written > 0
          order by
            ssd_blks_written desc
          limit {row_limit}
        ) a
        union
        select * from (
          select
            *
          from
            q_data
          where
            ssd_temp_blks_read > 0
          order by
            ssd_temp_blks_read desc
          limit {row_limit}
        ) a
        union
        select * from (
          select
            *
          from
            q_data
          where
            ssd_temp_blks_written > 0
          order by
            ssd_temp_blks_written desc
          limit {row_limit}
        ) a
'''.format(row_limit=ROW_LIMITS_FOR_SINGLE_ORDERING_CRITERIONS)

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get)
        for d in data:
            d['ssd_host_id'] = self.host_id
            d['ssd_query'] = StatStatementsGatherer.perform_regex_transformations(d['ssd_query'])
            if 'ssd_query_id' not in d:
                d['ssd_query_id'] = d['ssd_query'].__hash__()

        return data

    @staticmethod
    def perform_regex_transformations(some_string):
        ret_string = some_string
        for tf_rule in QUERY_TRANSFORMATION_REGEXES:
            ret_string = tf_rule['pattern'].sub(tf_rule['replace'], ret_string)
        return ret_string


if __name__ == '__main__':
    print(StatStatementsGatherer.perform_regex_transformations('a/*\t\nb*/c'))
