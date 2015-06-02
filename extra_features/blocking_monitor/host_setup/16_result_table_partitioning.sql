CREATE OR REPLACE FUNCTION z_blocking.drop_old_partitions(p_table_base_name text, p_drop_timestamp timestamp with time zone)
RETURNS void AS $$
DECLARE
    l_week_to_drop text := to_char(p_drop_timestamp, 'YYYYIW');
    l_sql text;
    l_table_to_drop text;
    c record;
BEGIN
    l_table_to_drop := format('%s_%s', p_table_base_name, l_week_to_drop);
    l_sql := format('select table_name from information_schema.tables where table_schema = ''%s'' and table_name <= ''%s'' and table_name like E''%s\\_%%''',
            'z_blocking', l_table_to_drop, p_table_base_name);
    FOR c in EXECUTE l_sql
    LOOP
      l_sql := 'drop table if exists z_blocking.'|| c.table_name;
      RAISE WARNING 'dropping z_blocking.%', c.table_name;
      EXECUTE l_sql;
    END LOOP;
END;
$$ language plpgsql;



CREATE OR REPLACE FUNCTION z_blocking.blocking_locks_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_table_name text := 'blocking_locks';
  l_table_full_name text := 'z_blocking.blocking_locks';
  l_partition_key text := 'bl_timestamp';
  l_weeks_to_keep interval := '4 weeks';
  l_partition_full_name text;
  l_sql text;
  l_partition_week text;
  l_date_check_lower timestamp;
  l_date_check_upper timestamp;
  l_table_to_drop text;
BEGIN
  l_partition_week := to_char(NEW.bl_timestamp, 'YYYYIW');
  l_date_check_lower = date_trunc('week', NEW.bl_timestamp);
  l_date_check_upper = date_trunc('week', NEW.bl_timestamp) + '1 week'::interval;
  l_partition_full_name := format('z_blocking.%s_%s', l_table_name, l_partition_week);

  IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = l_table_name||'_'||l_partition_week AND schemaname = 'z_blocking')
  THEN
    l_sql := format('CREATE TABLE %s (LIKE %s INCLUDING ALL) INHERITS (%s)', l_partition_full_name, l_table_full_name, l_table_full_name);
    --RAISE WARNING '%', l_sql;
    EXECUTE l_sql;

    l_sql := format('ALTER TABLE %s ADD CONSTRAINT date_range CHECK (%s >= ''%s'' AND %s < ''%s'')',
                    l_partition_full_name, l_partition_key, l_date_check_lower, l_partition_key, l_date_check_upper);
    --RAISE WARNING '%', l_sql;
    EXECUTE l_sql;

    -- cleanup. keeping 4 weekly partitions
    PERFORM z_blocking.drop_old_partitions(l_table_name, l_date_check_lower - l_weeks_to_keep);
  END IF;

  l_sql := 'INSERT INTO '|| l_partition_full_name ||' SELECT (' || quote_literal( NEW ) || '::'||l_table_full_name || ').*' ;
  EXECUTE l_sql;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS blocking_locks_part_ins ON z_blocking.blocking_locks;
CREATE TRIGGER blocking_locks_part_ins BEFORE INSERT ON z_blocking.blocking_locks
FOR EACH ROW EXECUTE PROCEDURE z_blocking.blocking_locks_insert_trigger();



CREATE OR REPLACE FUNCTION z_blocking.blocking_processes_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_table_name text := 'blocking_processes';
  l_table_full_name text := 'z_blocking.blocking_processes';
  l_partition_key text := 'bp_timestamp';
  l_weeks_to_keep interval := '4 weeks';
  l_partition_full_name text;
  l_sql text;
  l_partition_week text;
  l_date_check_lower timestamp;
  l_date_check_upper timestamp;
  l_table_to_drop text;
BEGIN
  l_partition_week := to_char(NEW.bp_timestamp, 'YYYYIW');
  l_date_check_lower = date_trunc('week', NEW.bp_timestamp);
  l_date_check_upper = date_trunc('week', NEW.bp_timestamp) + '1 week'::interval;
  l_partition_full_name := format('z_blocking.%s_%s', l_table_name, l_partition_week);

  IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = l_table_name||'_'||l_partition_week AND schemaname = 'z_blocking')
  THEN
    l_sql := format('CREATE TABLE %s (LIKE %s INCLUDING ALL) INHERITS (%s)', l_partition_full_name, l_table_full_name, l_table_full_name);
    --RAISE WARNING '%', l_sql;
    EXECUTE l_sql;

    l_sql := format('ALTER TABLE %s ADD CONSTRAINT date_range CHECK (%s >= ''%s'' AND %s < ''%s'')',
                    l_partition_full_name, l_partition_key, l_date_check_lower, l_partition_key, l_date_check_upper);
    --RAISE WARNING '%', l_sql;
    EXECUTE l_sql;

    -- cleanup. keeping 4 weekly partitions
    PERFORM z_blocking.drop_old_partitions(l_table_name, l_date_check_lower - l_weeks_to_keep);

  END IF;

  l_sql := 'INSERT INTO '|| l_partition_full_name ||' SELECT (' || quote_literal( NEW ) || '::'||l_table_full_name || ').*' ;
  EXECUTE l_sql;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS blocking_processes_part_ins ON z_blocking.blocking_processes;
CREATE TRIGGER blocking_processes_part_ins BEFORE INSERT ON z_blocking.blocking_processes
FOR EACH ROW EXECUTE PROCEDURE z_blocking.blocking_processes_insert_trigger();