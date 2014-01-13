from __future__ import print_function

import DataDB
import json
from collections import defaultdict

class Export(object):
    """
    Class for exporting performace metrics to other tools
    """
    
    def topsprocsbycalls(self):
        q = """
            select
                host_db_export_name as db,
                sproc_name,
                count
            from (
                select
                    *,
                    row_number() over(partition by host_db_export_name order by count desc)
                from (
                    select
                        host_db_export_name,
                        substring(sproc_name, 1, position ('(' in sproc_name)-1) as sproc_name,
                        count(*)
                    from sprocs
                    join sproc_performance_data on sp_sproc_id = sproc_id
                    join hosts on host_id = sproc_host_id 
                    where sp_timestamp > now() - '7days'::interval
                    and host_db_export_name is not null
                    group by 1, 2
                ) a
            ) b
            where row_number <= 10
            order by host_db_export_name, count desc
        """
        topsprocs = DataDB.execute(q)
        # print (topsprocs)
        retdict=defaultdict(list)
        for r in topsprocs:
            retdict[r['db']].append(r['sproc_name'])
        return json.dumps(retdict)
    
    def topsprocsbyruntime(self):
        q = """
            select
                host_db_export_name as db,
                sproc_name,
                total_runtime
            from (
                select
                    *,
                    row_number() over(partition by host_db_export_name order by total_runtime desc)
                from (
                    select
                        host_db_export_name,
                        substring(sproc_name, 1, position ('(' in sproc_name)-1) as sproc_name,
                        max(sp_total_time)-min(sp_total_time) as total_runtime
                    from sprocs
                    join sproc_performance_data on sp_sproc_id = sproc_id
                    join hosts on host_id = sproc_host_id 
                    where sp_timestamp > now() - '7days'::interval
                    and host_db_export_name is not null
                    group by 1, 2
                ) a
            ) b
            where row_number <= 10
            order by host_db_export_name, total_runtime desc
        """
        topbyruntime = DataDB.execute(q)
        retdict=defaultdict(list)
        for r in topbyruntime:
            retdict[r['db']].append(r['sproc_name'])
        return json.dumps(retdict)

    topsprocsbycalls.exposed = True
    topsprocsbyruntime.exposed = True

