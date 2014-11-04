\i ../schema/04_monitor_api.sql

/*
append the configuration for bgstat gatherer to existing configuration for all hosts
*/
update monitor_data.hosts out
   set host_settings = (
            select E'{\n' || array_to_string(array_agg( '"'||key||'" : '|| value), E', \n') ||  E'\n}'
              from ( select *
                       from json_each( (select host_settings::json
                                          from monitor_data.hosts i
                                         where out.host_id = i.host_id) ) a 
                                         union all 
                                        select 'statBgwriterGatherInterval', '60') b)
 where host_settings not like '%statBgwriterGatherInterval%';

