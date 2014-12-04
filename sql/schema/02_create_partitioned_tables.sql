RESET role;
BEGIN;

CREATE SCHEMA IF NOT EXISTS monitor_data_partitions AUTHORIZATION pgobserver_gatherer;

GRANT USAGE ON SCHEMA monitor_data_partitions TO pgobserver_frontend;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_gatherer IN SCHEMA monitor_data_partitions GRANT SELECT ON TABLES to pgobserver_frontend;


set role to pgobserver_gatherer;
SET search_path = monitor_data, public;

--DROP FUNCTION IF EXISTS monitor_data.create_partitioned_tables(text, text, text, text, timestamp without time zone);
CREATE OR REPLACE FUNCTION monitor_data.create_partitioned_table(
    IN p_table_name text, IN p_column_prefix text, IN p_yyyymm text, IN p_host_id text, IN p_timestamp timestamp without time zone)
  RETURNS VOID AS
$BODY$
DECLARE
  l_create_string text;
  l_table_to_create text;
  l_table_full_name text;
  l_table_name_yyyymm text;
  l_date_check_lower text := date_trunc('month', p_timestamp);
  l_date_check_upper text := date_trunc('month', p_timestamp + '1 month'::interval);
  l_host_id_column text := p_column_prefix || '_host_id';
  l_timestamp_column text := p_column_prefix || '_timestamp';
BEGIN
    l_table_name_yyyymm := p_table_name || '_' || p_yyyymm;
    IF p_host_id IS NULL THEN
      l_table_full_name := l_table_name_yyyymm;
    ELSE
      l_table_full_name := l_table_name_yyyymm || '_' || p_host_id;
    END IF;

    --RAISE WARNING 'p_table_name = %, p_column_prefix = %, p_yyyymm = %, p_host_id = %, p_timestamp = %', p_table_name, p_column_prefix, p_yyyymm, p_host_id, p_timestamp;
    --RAISE WARNING 'l_table_full_name = %', l_table_full_name;

    PERFORM 1   --is table already there?
    FROM pg_tables
    WHERE tablename = l_table_full_name
    AND schemaname = 'monitor_data_partitions';

    IF FOUND THEN
      RAISE WARNING '% already existing!', l_table_full_name;
      RETURN;
    END IF;
    RAISE WARNING 'creating table %', l_table_full_name;

    --if inserting into host-specific table create the parent (date-based) table if not there
    IF p_host_id IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1
                        FROM pg_tables
                       WHERE tablename = l_table_name_yyyymm
                         AND schemaname = 'monitor_data_partitions')
        THEN
            RAISE WARNING 'creating parent table: %', l_table_name_yyyymm;
            PERFORM monitor_data.create_partitioned_table(p_table_name, p_column_prefix, p_yyyymm, NULL, p_timestamp);
        END IF;
        l_create_string := 'CREATE TABLE monitor_data_partitions.' || l_table_full_name || ' (LIKE monitor_data_partitions.' || l_table_name_yyyymm || ' INCLUDING ALL)
          INHERITS (monitor_data_partitions.' || l_table_name_yyyymm || ')';
    ELSE
      l_create_string := 'CREATE TABLE monitor_data_partitions.' || l_table_full_name || ' (LIKE monitor_data.' || p_table_name || ' INCLUDING ALL) INHERITS (monitor_data.' || p_table_name || ')';
    END IF;

    --RAISE WARNING 'l_create_string: %', l_create_string;
    EXECUTE l_create_string;


    IF p_host_id IS NOT NULL THEN
      l_create_string := 'ALTER TABLE monitor_data_partitions.' || l_table_full_name || ' ADD CHECK ('||l_host_id_column||'='||p_host_id||')';
    ELSE
      l_create_string := $$ALTER TABLE monitor_data_partitions.$$ || l_table_full_name || $$ ADD CONSTRAINT date_range CHECK ($$
        || l_timestamp_column || $$ >= '$$ || l_date_check_lower || $$' AND $$ || l_timestamp_column || $$ < ' $$ || l_date_check_upper || $$')$$ ;
    END IF;

    --RAISE WARNING 'adding constraint: %', l_create_string;
    EXECUTE l_create_string;

END;
$BODY$
LANGUAGE plpgsql; --SECURITY DEFINER
GRANT EXECUTE ON FUNCTION monitor_data.create_partitioned_table(text, text, text, text, timestamp without time zone) TO pgobserver_gatherer;




--DROP FUNCTION IF EXISTS monitor_data.clean_up_partitioned_tables();

CREATE OR REPLACE FUNCTION monitor_data.clean_up_partitioned_tables(p_yyyymm_to_keep text, p_dry_run boolean default true)
  RETURNS VOID AS
$BODY$
DECLARE
  --l_months_to_keep interval := '6 months';
  l_drop_string text;
  l_rec record;
BEGIN

  IF length(p_yyyymm_to_keep) != 6 THEN
    RAISE EXCEPTION 'p_yyyymm_to_keep is meant to be in YYYYMM format';
  END IF;

  FOR l_rec IN (
    SELECT tablename, regexp_replace(tablename, E'\\D+','') as date
      FROM pg_tables
     WHERE schemaname = 'monitor_data_partitions'
       AND regexp_replace(tablename, E'\\D+','') < p_yyyymm_to_keep
       AND length(regexp_replace(tablename, E'\\D+','')) = 6
  )
  LOOP
    l_drop_string := 'DROP TABLE monitor_data_partitions.' || l_rec.tablename || ' CASCADE';
    if p_dry_run then
      RAISE WARNING 'would drop: %', l_rec.tablename;
    else
      RAISE WARNING 'dropping: %', l_rec.tablename;
      EXECUTE l_drop_string;
    end if;

  END LOOP;

END;
$BODY$
LANGUAGE plpgsql;

/*

cleans up older data partitions - should run from somekind of monthly cron

*/
CREATE OR REPLACE FUNCTION monitor_data.clean_up_partitioned_tables(p_months_to_keep int default 6, p_dry_run boolean default true)
  RETURNS VOID AS
$BODY$
DECLARE
  l_drop_string text;
  l_yyyymm_to_keep text;
  l_rec record;
BEGIN

  IF p_months_to_keep < 2 THEN
    RAISE EXCEPTION 'p_months_to_keep needs to be >= 2';
  END IF;

  l_yyyymm_to_keep := to_char(date_trunc('month', now() - (p_months_to_keep||' months')::interval ), 'YYYYMM');

  FOR l_rec IN (
    SELECT tablename, regexp_replace(tablename, E'\\D+','') as date
      FROM pg_tables
     WHERE schemaname = 'monitor_data_partitions'
       AND regexp_replace(tablename, E'\\D+','') < l_yyyymm_to_keep
       AND length(regexp_replace(tablename, E'\\D+','')) = 6
  )
  LOOP
    l_drop_string := 'DROP TABLE monitor_data_partitions.' || l_rec.tablename || ' CASCADE';
    if p_dry_run then
      RAISE WARNING 'would drop: %', l_rec.tablename;
    else
      RAISE WARNING 'dropping: %', l_rec.tablename;
      EXECUTE l_drop_string;
    end if;

  END LOOP;

END;
$BODY$
LANGUAGE plpgsql;

COMMIT;