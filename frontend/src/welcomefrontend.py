import hosts
import tplE
import datetime
import datadb
from collections import OrderedDict

class WelcomeFrontend(object):

    def get_last_loads_and_sizes(self):
        yyyymm = datetime.datetime.now().strftime('%Y%m')
        sql = """
                with
                q_last_5m_loads as (
                  select
                    distinct on (load_host_id)
                    load_host_id as host_id,
                    round(load_5min_value/100.0,1) as last_5min_load
                  from
                    monitor_data_partitions.host_load_""" + yyyymm + """
                  where
                    load_timestamp > now() - '1 day'::interval
                  order by
                    load_host_id, load_timestamp desc
                ),
                q_size as (
                  select
                    distinct on (tsda_host_id)
                    tsda_host_id as host_id,
                    pg_size_pretty(tsda_db_size) last_agg_size
                  from
                    monitor_data_partitions.table_size_data_agg_""" + yyyymm + """
                  where
                   tsda_timestamp > now() - '1 day'::interval
                  order by
                    tsda_host_id, tsda_timestamp desc
                )
                select
                  h.host_id,
                  h.host_ui_shortname,
                  h.host_ui_longname,
                  hg.group_name,
                  coalesce(q_last_5m_loads.last_5min_load::text, '-') as last_5min_load,
                  coalesce(q_size.last_agg_size::text, '-') as last_agg_size
                from
                  hosts h
                  left join
                    q_last_5m_loads
                      on q_last_5m_loads.host_id = h.host_id
                  left join
                    q_size
                      on q_size.host_id = h.host_id
                  left join
                    host_groups hg
                      on hg.group_id = h.host_group_id
                where
                  host_enabled
                order by
                  case when hg.group_id = 0 then 0 else 1 end, hg.group_name nulls first, h.host_ui_longname
        """
        sql_no_aggr = """
            select
              h.host_id,
              h.host_ui_shortname,
              h.host_ui_longname,
              hg.group_name,
              coalesce((select round(load_5min_value/100.0,1)::text from host_load where load_host_id = h.host_id and load_timestamp > (now() - '1day'::interval) order by load_timestamp desc limit 1), '-') as last_5min_load,
              '-' as last_agg_size
            from
              hosts h
              left join
              host_groups hg on hg.group_id = h.host_group_id
            where
              host_enabled
            order by
              case when hg.group_id = 0 then 0 else 1 end, hg.group_name nulls first, h.host_ui_longname
        """

        if tplE._settings.get('run_aggregations'):
            return datadb.execute(sql)
        else:
            return datadb.execute(sql_no_aggr) # no "last db size"


    def index(self):
        hosts_with_last_stats_by_groups = OrderedDict()

        if len(hosts.getHosts()) > 0:
            hosts_with_last_stats = self.get_last_loads_and_sizes()
            for h in hosts_with_last_stats:
                if h['group_name'] not in hosts_with_last_stats_by_groups:
                    hosts_with_last_stats_by_groups[h['group_name']] = []
                hosts_with_last_stats_by_groups[h['group_name']].append(h)

        tmpl = tplE.env.get_template('welcome.html')
        return tmpl.render(hosts_with_last_stats_by_groups=hosts_with_last_stats_by_groups,
                           target='World')

    index.exposed = True