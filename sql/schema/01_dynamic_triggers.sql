SET ROLE TO pgobserver_gatherer;
SET search_path = monitor_data, public;

DROP FUNCTION IF EXISTS monitor_data.create_partitioning_trigger_for_table(text, text);

CREATE OR REPLACE FUNCTION monitor_data.create_partitioning_trigger_for_table(p_table_name text, p_column_prefix text)
returns void
as $SPROC$
declare

l_trg_func text := $SQL$
CREATE OR REPLACE FUNCTION monitor_data.%1$s_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_yyyymm text;
  l_partitioned_table_name text;
  l_sql text;
BEGIN
  l_yyyymm := to_char(NEW.%2$s_timestamp, 'YYYYMM');

  IF NEW.%2$s_host_id IS NULL THEN
    l_partitioned_table_name := '%1$s_' || l_yyyymm;
  ELSE
    l_partitioned_table_name := '%1$s_' || l_yyyymm || '_'|| NEW.%2$s_host_id::text;
  END IF;
  --RAISE WARNING 'l_partitioned_table_name: %%', l_partitioned_table_name;

  IF NOT EXISTS (SELECT 1
                   FROM pg_tables
                  WHERE tablename = l_partitioned_table_name
                    AND schemaname = 'monitor_data_partitions')
  THEN
    PERFORM monitor_data.create_partitioned_table('%1$s', '%2$s', l_yyyymm, NEW.%2$s_host_id::text, NEW.%2$s_timestamp);
  END IF;

  l_sql := 'INSERT INTO monitor_data_partitions.'|| l_partitioned_table_name ||' SELECT (' || quote_literal( NEW ) || '::monitor_data_partitions.'||l_partitioned_table_name || ').*' ;
  EXECUTE l_sql;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql; -- SECURITY DEFINER
$SQL$;

l_trg text :=
$SQL$
CREATE TRIGGER %1$s_insert_trigger
BEFORE INSERT ON monitor_data.%1$s
FOR EACH ROW EXECUTE PROCEDURE monitor_data.%1$s_insert_trigger();
$SQL$;

begin
  --raise warning '%',  format(l_trg_func, p_table_name, p_column_prefix);
  execute format(l_trg_func, p_table_name, p_column_prefix);
  --raise warning '%',  format(l_trg, p_table_name);
  execute format(l_trg, p_table_name);
end;
$SPROC$ language plpgsql;



DO $$
DECLARE
  l_tables text[] := array['sproc_performance_data', 'table_io_data', 'table_size_data', 'host_load', 'index_usage_data',
                            'schema_usage_data', 'stat_statements_data', 'stat_database_data', 'stat_bgwriter_data'];
  l_table_prefixes text[] := array['sp', 'tio', 'tsd', 'load', 'iud',
                            'sud', 'ssd', 'sdd', 'sbd'];
BEGIN
  FOR i IN 1.. array_upper(l_tables, 1)
  LOOP
    RAISE WARNING 'dropping triggers for: %', l_tables[i];
    EXECUTE 'DROP TRIGGER IF EXISTS ' || l_tables[i] || '_insert_trigger ON ' || l_tables[i];
    RAISE WARNING 'creating triggers for: %', l_tables[i];
    PERFORM monitor_data.create_partitioning_trigger_for_table(l_tables[i], l_table_prefixes[i]);
  END LOOP;
END;
$$
LANGUAGE plpgsql;
