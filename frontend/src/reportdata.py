from __future__ import print_function
import datadb
import psycopg2
import psycopg2.extras
import hosts
from collections import OrderedDict
from collections import defaultdict



def getLoadReportData(hostId=None):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = """
            with q as (
                select 
                  tsd_host_id as host_id
                , week
                , (select round((sum(tsd_table_size) + sum(tsd_index_size))/(1024*1024*1024)::numeric,1)::text --in GB
                     from monitor_data.table_size_data
                    where tsd_timestamp = max) as db_size 
                from (
                    select
                        tsd_host_id
                      , extract(week from tsd_timestamp) as week               
                      , max(tsd_timestamp)
                    from  monitor_data.table_size_data
                    where tsd_timestamp > ('now'::timestamp - '9 weeks'::interval)
                    and (%s is null or tsd_host_id = %s)
                    group by tsd_host_id, extract(week from tsd_timestamp)
                ) a
            )
            select 
                  load_host_id AS id,
                  extract(week from load_timestamp)::text AS kw,
                  round(avg(load_15min_value)/100,2) AS avg,
                  round(max(load_15min_value)/100,2) AS max,
                  to_char(min(load_timestamp::date),'dd.mm.YYYY') AS min_date,
                  to_char(max(load_timestamp::date),'dd.mm.YYYY') AS max_date,
                  min(load_timestamp::date) AS sort_date,
                  max(q.db_size) as db_size,
                  round((max(xlog_location_mb) - min(xlog_location_mb)) / 1024.0, 1)  as wal_written
             from monitor_data.host_load hl
                , monitor_data.hosts h
                , q
            where h.host_id = hl.load_host_id
              and host_enabled
              and load_timestamp > ('now'::timestamp - '9 weeks'::interval)
              and extract(dow from load_timestamp) IN(1,2,3,4,5)                      
              and q.host_id = load_host_id
              and q.week = extract(week from load_timestamp)
              and (%s is null or hl.load_host_id = %s)
            group by load_host_id, extract(week from load_timestamp)
            order by 1 ASC,7 DESC
            """
    cur.execute(query, (hostId,hostId,hostId,hostId))

    data = defaultdict(list)

    lastRR = None

    for r in cur:

        rr = {'id' : r['id'],
              'avg' : r['avg'],
              'max' : r['max'],
              'min_date' : r['min_date'],
              'max_date' : r['max_date'],
              'db_size' : r['db_size'],
              'wal_written' : r['wal_written'],
              'trendAvg': 0,
              'trendMax': 0,
              'kw' : r['kw']
              }

        if lastRR != None and lastRR['id']==rr['id']:
            if lastRR['max'] < r['max']:
                lastRR['trendMax'] = -1
            elif lastRR['max'] > r['max']:
                lastRR['trendMax'] = 1

            if lastRR['avg'] < r['avg']:
                lastRR['trendAvg'] = -1
            elif lastRR['avg'] > r['avg']:
                lastRR['trendAvg'] = 1

            if lastRR['db_size'] < r['db_size']:
                lastRR['trendSize'] = -1
            elif lastRR['db_size'] > r['db_size']:
                lastRR['trendSize'] = 1

            if lastRR['wal_written'] < r['wal_written']:
                lastRR['trendWal'] = -1
            elif lastRR['wal_written'] > r['wal_written']:
                lastRR['trendWal'] = 1

        data[int(r['id'])].append(rr);
        lastRR = rr

    cur.close()
    conn.close()

    return sorted(data.values(), key = lambda x : hosts.hosts[x[0]['id']]['uishortname'])


def getTablePerformanceIssues(hostname, date_from, date_to):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""select * from monitor_data.get_table_threshold_sinners_for_period(%s,%s,%s)""", (hostname, date_from, date_to))
    data = [] # cur.fetchall()
    for r in cur:
        row = {'host_name' : r['host_name'],
              'host_id' : r['host_id'],
              'schema_name' : r['schema_name'],
              'table_name' : r['table_name'],
              'day' : r['day'],
              'scan_change_pct' : r['scan_change_pct'],
              'scans1': r['scans1'],
              'scans2': r['scans2'],
              'size1': r['size1'],
              'size2': r['size2'],
              'size_change_pct': r['size_change_pct'],
              'allowed_seq_scan_pct': r['allowed_seq_scan_pct'],
              }
        data.append(row)
    cur.close()
    conn.close()
    return data

def getApiPerformanceIssues(hostname, api_from, api_to):
    conn = datadb.getDataConnection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""select * from monitor_data.get_sproc_threshold_sinners_for_release(%s,%s,%s)""", (hostname, api_from, api_to))
    data = [] # cur.fetchall()
    for r in cur:
        row = {'host_name' : r['host_name'],
              'host_id' : r['host_id'],
              'sproc_schema' : r['sproc_schema'],
              'sproc_name' : r['sproc_name'],
              'calltime_change_pct' : r['calltime_change_pct'],
              'share_on_total_runtime' : r['share_on_total_runtime'],
              'execution_avg1': r['execution_avg1'],
              'execution_avg2': r['execution_avg2'],
              'calls1': r['calls1'],
              'calls2': r['calls2'],
              'callscount_change_pct': r['callscount_change_pct'],
              'allowed_runtime_growth_pct': r['allowed_runtime_growth_pct'],
              'allowed_share_on_total_runtime_pct': r['allowed_share_on_total_runtime_pct'],
              }
        data.append(row)
    cur.close()
    conn.close()
    return data

def getIndexIssues(hostname):
    q_invalid = """
        SELECT
        *,
        CASE WHEN indexes_size_bytes = 0 THEN 0 ELSE round((index_size_bytes::numeric / indexes_size_bytes::numeric)*100,1) END AS pct_of_tables_index_space,
        pg_size_pretty(total_marked_index_size_bytes::bigint) AS total_marked_index_size
        FROM (
                SELECT
                %s as host_name,
                %s as host_id,
                schemaname||'.'||relname AS table_full_name,
                schemaname||'.'||indexrelname AS index_full_name,
                index_size_bytes,
                indexes_size_bytes,
                pg_size_pretty(index_size_bytes) AS index_size,
                pg_size_pretty(indexes_size_bytes) AS indexes_size,
                pg_size_pretty(table_size_bytes) AS table_size,
                sum(index_size_bytes) over () AS total_marked_index_size_bytes
                FROM
                (
                  SELECT quote_ident(schemaname) as schemaname,
                         quote_ident(relname) as relname,
                         quote_ident(indexrelname) as indexrelname,
                         pg_relation_size(i.indexrelid) AS index_size_bytes,
                         pg_indexes_size(i.relid) AS indexes_size_bytes,                 
                         pg_relation_size(i.relid) AS table_size_bytes
                  FROM pg_stat_user_indexes i
                  JOIN pg_index USING(indexrelid) 
                  WHERE NOT indisvalid
                ) a                
        ) b 
        ORDER BY index_size_bytes DESC, index_full_name
    """
    q_unused = """
        SELECT
        *,
        pg_size_pretty(total_marked_index_size_bytes::bigint) AS total_marked_index_size
        FROM (
          SELECT
          *,
          pg_size_pretty(index_size_bytes) AS index_size,
          pg_size_pretty(indexes_size_bytes) AS indexes_size,
          pg_size_pretty(table_size_bytes) AS table_size,
          CASE WHEN indexes_size_bytes = 0 THEN 0 ELSE round((index_size_bytes::numeric / indexes_size_bytes::numeric)*100,1) END AS pct_of_tables_index_space,
          sum(index_size_bytes) over () AS total_marked_index_size_bytes
          FROM (
          SELECT   %s as host_name,
                   %s as host_id,
                   quote_ident(schemaname)||'.'||quote_ident(relname) AS table_full_name,
                   quote_ident(schemaname)||'.'||quote_ident(indexrelname) AS index_full_name,
                   pg_relation_size(i.indexrelid) as index_size_bytes,
                   pg_indexes_size(i.relid) AS indexes_size_bytes,
                   pg_relation_size(i.relid) AS table_size_bytes,
                   idx_scan AS scans
              FROM pg_stat_user_indexes i 
              JOIN pg_index USING(indexrelid) 
              WHERE NOT indisunique
              AND NOT schemaname LIKE ANY (ARRAY['tmp%%','temp%%'])
          ) a
          WHERE index_size_bytes > %s
          AND scans <= %s          
        ) b
        ORDER BY scans, index_size_bytes DESC
    """
    q_duplicate = """
        SELECT %s AS host_name,
               %s as host_id,
               n.nspname||'.'||ci.relname AS index_full_name,
               n.nspname||'.'||ct.relname AS table_full_name,
               pg_size_pretty(pg_total_relation_size(ct.oid)) AS table_size,
               pg_total_relation_size(ct.oid) AS table_size_bytes,
               n.nspname AS schema_name,
               index_names,
               def,
               count
        FROM (
          select regexp_replace(replace(pg_get_indexdef(i.indexrelid),c.relname,'X'), '^CREATE UNIQUE','CREATE') as def,
                 max(indexrelid) as indexrelid,
                 max(indrelid) as indrelid,
                 count(1),
                 array_agg(relname::text) as index_names
            from pg_index i
            join pg_class c
              on c.oid = i.indexrelid
           where indisvalid
           group 
              by regexp_replace(replace(pg_get_indexdef(i.indexrelid),c.relname,'X'), '^CREATE UNIQUE','CREATE')
          having count(1) > 1
        ) a
          JOIN pg_class ci
            ON ci.oid=a.indexrelid        
          JOIN pg_class ct
            ON ct.oid=a.indrelid
          JOIN pg_namespace n
            ON n.oid=ct.relnamespace
         ORDER
            BY count DESC, table_size_bytes DESC, schema_name, table_full_name
    """
    q_active_hosts="""
        select
            host_id,
            host_name,
            host_user,
            host_password,
            host_db
        from monitor_data.hosts
        where host_enabled
        and (%s = 'all' or host_name=%s)
        """
    q_indexing_thresholds="""select * from monitor_data.perf_indexes_thresholds"""
    data_invalid = []
    data_unused = []
    data_duplicate = []
    data_noconnect = []
    conn=None

    hosts = datadb.execute(q_active_hosts, (hostname, hostname))
    indexing_thresholds = datadb.execute(q_indexing_thresholds)[0]

    for h in hosts:
        try:
            conn = psycopg2.connect(host=h['host_name'], dbname=h['host_db'], user=h['host_user'], password=h['host_password'],connect_timeout='3')
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q_invalid, (h['host_name'], h['host_id']))
            data_invalid += cur.fetchall()
            cur.execute(q_unused, (h['host_name'], h['host_id'], indexing_thresholds['pit_min_size_to_report'], indexing_thresholds['pit_max_scans_to_report']))
            data_unused += cur.fetchall()
            cur.execute(q_duplicate, (h['host_name'], h['host_id']))
            data_duplicate += cur.fetchall()
        except Exception, e:
            print ('ERROR could not connect to {}:{}'.format(h['host_name'], e))
            data_noconnect.append({'host_id':h['host_id'],'host_name': h['host_name']})
        finally:
            if conn and not conn.closed:
                conn.close()
    
    data_invalid.sort(key=lambda x:x['index_size_bytes'],reverse=True)
    data_unused.sort(key=lambda x:x['index_size_bytes'],reverse=True)
    data_duplicate.sort(key=lambda x:x['table_size_bytes'],reverse=True)

    return {'invalid':data_invalid, 'duplicate':data_duplicate, 'unused':data_unused, 'noconnect':data_noconnect}


def get_unused_schemas(host_name, from_date, to_date, filter):
    sql = """
with
q_max_daily_timestamps as (
  select
    sud_host_id as host_id,
    sud_schema_name as schema_name,
    --sud_timestamp::date as day,
    max(sud_timestamp) as timestamp
  from
    monitor_data.schema_usage_data s
  where
    sud_timestamp between %s and %s
    and sud_host_id in (select host_id from monitor_data.hosts where host_name = %s or %s = 'all')
    and (not sud_schema_name like any (array['pg\_%%', '%%\_data'])
         and sud_schema_name not in ('public', '_v'))
    and sud_schema_name like '%%'||%s||'%%'
  group by
    sud_host_id, sud_schema_name, sud_timestamp::date
  order by
    sud_host_id, sud_schema_name, sud_timestamp::date
),
q_min_max as (
  select
    sud_host_id as host_id,
    sud_schema_name as schema_name,
    min(sud_timestamp),
    max(sud_timestamp)
  from
    monitor_data.schema_usage_data
  where
    sud_timestamp between %s and %s
    and sud_schema_name like '%%'||%s||'%%'
  group by
    sud_host_id, sud_schema_name
),
q_endofday_total_counts as (
  select
    host_id,
    schema_name,
    timestamp,
    sud_sproc_calls /* + max(sud_seq_scans)  + max(sud_idx_scans) */ + sud_tup_ins + sud_tup_upd + sud_tup_del as daily_total
  from
    monitor_data.schema_usage_data
    join
    q_max_daily_timestamps on sud_host_id = host_id and sud_schema_name = schema_name and sud_timestamp = timestamp
  order by
    1, 2, 3
)
select
  h.host_name,
  h.host_db,
  h.host_id,
  b.schema_name,
  mm.min,
  mm.max
from
  (
    select
      host_id,
      schema_name,
      bool_and(is_same_as_prev) is_unchanged
    from
      (
        select
          *,
          case
            when lag(daily_total) over w is null then true --1st day
            when lag(daily_total) over w > daily_total then true --stats reset/overflow
            else lag(daily_total) over w = daily_total
          end as is_same_as_prev
        from
          q_endofday_total_counts
        window w as
          (partition by host_id, schema_name  order by host_id, schema_name, timestamp)
      ) a
    group by
      host_id, schema_name
    order by
      host_id, schema_name
  ) b
  join
  monitor_data.hosts h on h.host_id = b.host_id
  join
  q_min_max mm on mm.host_id = h.host_id and mm.schema_name = b.schema_name
where
  is_unchanged
order by
  host_name, schema_name
        """

    unused = datadb.execute(sql, (from_date, to_date, host_name, host_name, filter, from_date, to_date, filter))
    return unused

def get_schema_usage_for_host(host_name, date1, date2, filter=''):
    sql = """
      select
        sud_schema_name as schema_name,
        sud_timestamp::date AS date,
        array[max(sud_sproc_calls) - min(sud_sproc_calls), (max(sud_seq_scans) + max(sud_idx_scans)) - (min(sud_seq_scans) + min(sud_idx_scans)),
          max(sud_tup_ins) - min(sud_tup_ins),  max(sud_tup_upd) - min(sud_tup_upd),  max(sud_tup_del) - min(sud_tup_del)] as daily_counts
      from
        monitor_data.schema_usage_data
      where
        sud_host_id = (select host_id from monitor_data.hosts where host_name = %s)
        and sud_timestamp between %s and %s
        and sud_schema_name like '%%'||%s||'%%'
      group by
        sud_schema_name, sud_timestamp::date
      order by
        1, 2
        """
    usage = datadb.execute(sql, (host_name, date1, date2, filter))
    ret = OrderedDict()
    for u in usage:
        if u['schema_name'] not in ret: ret[u['schema_name']] = []
        ret[u['schema_name']].append((u['date'], u['daily_counts']))
    return ret


def get_unused_schemas_drop_sql(host_name, date1, date2, filter=''):
    data = get_unused_schemas(host_name, date1, date2, filter)
    sb = []
    prev_db = None
    sb.append("do $$\nbegin")
    for d in data:
        if prev_db != d['host_db']:
            if prev_db:
                sb.append("  end if;")
            sb.append("\n  if current_database() = '{}' then".format(d['host_db']))
        sb.append("""    execute 'drop schema if exists {} cascade';""".format(d['schema_name']))
        prev_db = d['host_db']
    if len(data) > 0:
        sb.append("  end if;")
    sb.append("\nend;\n$$;\n")
    return "\n".join(sb)

def getLocksReport(host_name, date1, date2):
    # IN p_from_date timestamp, IN p_from_hour integer, IN p_to_date timestamp, IN p_is_ignore_advisory boolean DEFAULT true,
    # OUT host_name text, OUT total_time_ss bigint, OUT threads_count bigint, OUT incidents_count bigint, OUT blocked_query text, OUT one_blocking_query text
    q_locks = '''
        select * from monitor_data.blocking_last_day(%s, %s)
        where (host_name = %s or %s = 'all')
        and total_time_ss > 5
        order by host_name, incidents_count desc
    '''
    return datadb.execute(q_locks, (date1, date2, host_name, host_name))


def getStatStatements(host_name, date1=None, date2=None, order_by='1', limit='50', no_copy_ddl=True, min_calls='3'):
    order_by = int(order_by) + 1
    sql = '''
select
  query,
  calls,
  total_time,
  blks_read,
  blks_written,
  temp_blks_read,
  temp_blks_written,
  case when calls > 0 then round(total_time / calls::numeric) else null end as avg_runtime_ms,
  query_id
from (
select
  max(ssd_query) as query,
  max(ssd_calls) - min(ssd_calls) as calls,
  max(ssd_total_time) - min(ssd_total_time) as total_time,
  max(ssd_blks_read) - min(ssd_blks_read) as blks_read,
  max(ssd_blks_written) - min(ssd_blks_written) as blks_written,
  max(ssd_temp_blks_read) - min(ssd_temp_blks_read) as temp_blks_read,
  max(ssd_temp_blks_written) - min(ssd_temp_blks_written) as temp_blks_written,
  ssd_query_id as query_id
from
  monitor_data.stat_statements_data
  join
  monitor_data.hosts on ssd_host_id = host_id
where
  host_name = %s
  and ssd_timestamp >= coalesce(%s, current_date-1) and ssd_timestamp < coalesce(%s, now())
  and case when %s then not upper(ssd_query) like any(array['COPY%%', 'CREATE%%']) else true end
group by
  ssd_query_id
) a
where
   calls >= %s::int
order by ''' + str(order_by) + '''
  desc nulls last
limit ''' + limit
    return datadb.execute(sql, (host_name, date1, date2, True if no_copy_ddl else False, min_calls))


def getStatStatementsGraph(hostid, query_id, date1, date2):
    sql = """
select
  ssd_query_id as query_id,
  ssd_timestamp as timestamp,
  ssd_query as query,
  ssd_calls  as calls,
  ssd_total_time as total_time,
  ssd_blks_read as blks_read,
  ssd_blks_written as blks_written,
  ssd_temp_blks_read as temp_blks_read,
  ssd_temp_blks_written as temp_blks_written
from
  monitor_data.stat_statements_data
where
  ssd_host_id = %s
  and ssd_query_id = %s
  and ssd_timestamp between %s and %s
order by
  ssd_timestamp
    """
    return datadb.execute(sql, (hostid, query_id, date1, date2))


def getBloatedTablesForHostname(hostname, order_by='wasted_bytes', limit=50):
    sql = """
    SELECT * FROM zz_utils.get_bloated_tables(%s, %s)
    """
    host = hosts.getHostsDataForConnecting(hostname)[0]
    return datadb.executeOnHost(hostname, host['host_port'], host['host_db'], host['host_user'], host['host_password'], sql, (True if order_by == 'bloat_factor' else False, int(limit)))


def getBloatedIndexesForHostname(hostname, order_by=False, limit=50):
    sql = """
    SELECT * FROM zz_utils.get_bloated_indexes(%s, %s)
    """
    host = hosts.getHostsDataForConnecting(hostname)[0]
    return datadb.executeOnHost(hostname, host['host_port'], host['host_db'], host['host_user'], host['host_password'], sql, (True if order_by == 'bloat_factor' else False, int(limit)))

def apply_average(datarow, minutes, time_delta, keys_to_skip=[]):
    if time_delta.seconds <= 1 * 60:
        return datarow
    divisor = time_delta.seconds / float(minutes * 60)
    for key in datarow.keys():
        if key not in keys_to_skip:
            if datarow[key] <= 1:   # special handling not to miss really incremental stuff
                continue
            datarow[key] = long(round(datarow[key] / divisor))
    return datarow


def getDatabaseStatistics(hostid, days='8'):
    days += 'days'
    sql = """
        select
          sdd_timestamp,
          sdd_numbackends,
          sdd_xact_commit,
          sdd_xact_rollback,
          sdd_blks_read,
          sdd_blks_hit,
          sdd_temp_files,
          sdd_temp_bytes,
          sdd_deadlocks,
          sdd_blk_read_time,
          sdd_blk_write_time
        from
          monitor_data.stat_database_data
        where
          sdd_host_id = %s
          and sdd_timestamp >= current_date - %s::interval
        order by
          sdd_timestamp
    """
    data = datadb.execute(sql, (hostid, days))
    ret = []
    prev_row = None
    for row in data:
        rr = {}
        if prev_row:
            rr['timestamp'] = row['sdd_timestamp']

            rr['numbackends'] = row['sdd_numbackends']
            # commit_delta = max(row['sdd_xact_commit'] - prev_row['sdd_xact_commit'], 0) # max() is for cases where stats are reset
            rollback_delta = max(row['sdd_xact_rollback'] - prev_row['sdd_xact_rollback'], 0)
            rr['rollbacks'] = rollback_delta
            # blks_read_delta = max(row['sdd_blks_read'] - prev_row['sdd_blks_read'], 0)
            # blks_hit_delta = max(row['sdd_blks_hit'] - prev_row['sdd_blks_hit'], 0)
            # blk_read_time_delta = max(row['sdd_blk_read_time'] - prev_row['sdd_blk_read_time'], 0)
            # blk_write_time_delta = max(row['sdd_blk_write_time'] - prev_row['sdd_blk_write_time'], 0)
            #rr['temp_files'] = max(row['sdd_temp_files'] - prev_row['sdd_temp_files'], 0)
            rr['temp_files_bytes'] = max(row['sdd_temp_bytes'] - prev_row['sdd_temp_bytes'], 0)
            rr['deadlocks'] = max(row['sdd_deadlocks'] - prev_row['sdd_deadlocks'], 0)

            # if commit_delta + rollback_delta > 0:
            #     rr['rollback_ratio'] = round (rollback_delta / float(commit_delta + rollback_delta), 1)
            # else:
            #     rr['rollback_ratio'] = 0
            # if blks_read_delta + blks_hit_delta > 0:
            #     rr['buffers_miss_ratio'] = round (blks_read_delta / float(blks_read_delta + blks_hit_delta), 1)
            # else:
            #     rr['buffers_miss_ratio'] = 0
            # if blk_read_time_delta + blk_write_time_delta > 0:
            #     rr['write_time_ratio'] = round (blk_write_time_delta / float(blk_read_time_delta + blk_write_time_delta), 1)
            # else:
            #     rr['write_time_ratio'] = 0

            # was done in the data fetching query previously but this seems to be faster
            time_delta = row['sdd_timestamp'] - prev_row['sdd_timestamp']
            rr = apply_average(rr, 15, time_delta, keys_to_skip=['timestamp','numbackends', 'rollback_ratio','buffers_miss_ratio','write_time_ratio'])

            ret.append(rr)
        prev_row = row

    return ret


def getGetActiveFrontendAnnouncementIfAny():
    announcement = None
    sql = """
      SELECT fa_announcement_text FROM frontpage_announcement WHERE fa_validity_range @> now()::timestamp;
    """

    try:
        announcement = datadb.execute(sql)
        if announcement:
            announcement = announcement[0]['fa_announcement_text']
    except:
        print('Exception reading frontpage_announcement table. is it there?')

    return announcement


if __name__ == '__main__':
    # print (getLocksReport(None, None))
    # print (getStatStatements('localhost', None, None))

    # for i, x in enumerate(getDatabaseStatistics(1)):
    #     print (x)
    #     if i==10:
    #         break
    print (getGetActiveFrontendAnnouncementIfAny())