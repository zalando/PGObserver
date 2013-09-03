BEGIN;

DROP FUNCTION IF EXISTS monitor_data.create_partitioned_tables(text, text, timestamp);

CREATE OR REPLACE FUNCTION monitor_data.create_partitioned_tables(in year text, in month text, in startdate timestamp)
  RETURNS VOID AS
$BODY$
DECLARE
  l_list_of_tables text[] := array['sproc_performance_data', 'table_io_data', 'table_size_data'];
  l_list_of_date_columns text[] := array['sp_timestamp', 'tio_timestamp', 'tsd_timestamp'];
  l_create_string text;
  l_table_to_create text;
  l_table_full_name text;
  table_to_create text;
  l_date_check_lower text := date_trunc('month', startdate);
  l_date_check_upper text := date_trunc('month', startdate + '1 month'::interval);
BEGIN
  IF length(month) = 1 THEN
    month := '0' || month;
  END IF;

  FOR i IN 1.. array_upper(l_list_of_tables, 1)
  LOOP
    l_table_full_name := l_list_of_tables[i] || '_' || year || month;
    --RAISE WARNING 'creating table%', l_table_full_name;

    PERFORM 1
    FROM pg_tables
    WHERE tablename = l_table_full_name
    AND schemaname = 'monitor_data';

    IF FOUND THEN
      RAISE WARNING '% already existing!', l_table_full_name;
      CONTINUE;
    END IF;

    l_create_string := 'CREATE TABLE monitor_data.' || l_table_full_name || ' (LIKE ' || l_list_of_tables[i] || ' INCLUDING ALL) INHERITS (' || l_list_of_tables[i] || ')';
    RAISE WARNING 'running: %', l_create_string;
    EXECUTE l_create_string;

    l_create_string := $$ALTER TABLE monitor_data.$$ || l_table_full_name || $$ ADD CONSTRAINT date_range CHECK ($$
      || l_list_of_date_columns[i] || $$ >= '$$ || l_date_check_lower || $$' AND $$ || l_list_of_date_columns[i] || $$ < ' $$ || l_date_check_upper || $$')$$ ;
    --RAISE WARNING 'running: %', l_create_string;
    EXECUTE l_create_string;

  END LOOP;

END;
$BODY$
LANGUAGE plpgsql;



DROP TRIGGER IF EXISTS sproc_performance_data_insert_trigger ON monitor_data.sproc_performance_data;
DROP FUNCTION IF EXISTS monitor_data.sproc_performance_data_insert_trigger();

CREATE OR REPLACE FUNCTION monitor_data.sproc_performance_data_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_year text;
  l_month text;
  l_partitioned_table_name text;
  l_row_as_text text;
BEGIN
  l_month := to_char(NEW.sp_timestamp, 'MM');
  l_year := to_char(NEW.sp_timestamp, 'YYYY');
  l_partitioned_table_name := 'sproc_performance_data_' || l_year || l_month;
  l_row_as_text := NEW;
  --RAISE WARNING 'l_partitioned_table_name: %', l_partitioned_table_name;

  IF NOT EXISTS (SELECT 1
                   FROM pg_tables
                  WHERE tablename = l_partitioned_table_name
                    AND schemaname = 'monitor_data')
  THEN
    PERFORM monitor_data.create_partitioned_tables(l_year, l_month, NEW.sp_timestamp);
  END IF;

  EXECUTE 'INSERT INTO '|| l_partitioned_table_name ||' SELECT (' || quote_literal( NEW ) || '::monitor_data.'||l_partitioned_table_name || ').*' ;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS table_io_data_insert_trigger ON monitor_data.table_io_data;
DROP FUNCTION IF EXISTS monitor_data.table_io_data_insert_trigger();

CREATE OR REPLACE FUNCTION monitor_data.table_io_data_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_year text;
  l_month text;
  l_partitioned_table_name text;
BEGIN
  l_month := to_char(NEW.tio_timestamp, 'MM');
  l_year := to_char(NEW.tio_timestamp, 'YYYY');
  l_partitioned_table_name := 'table_io_data_' || l_year || l_month;
  --RAISE WARNING 'l_partitioned_table_name: %', l_partitioned_table_name;

  IF NOT EXISTS (SELECT 1
                   FROM pg_tables
                  WHERE tablename = l_partitioned_table_name
                    AND schemaname = 'monitor_data')
  THEN
    PERFORM monitor_data.create_partitioned_tables(l_year, l_month, NEW.tio_timestamp);
  END IF;


  EXECUTE 'INSERT INTO '|| l_partitioned_table_name ||' SELECT (' || quote_literal( NEW ) || '::monitor_data.'||l_partitioned_table_name || ').*' ;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS table_size_data_insert_trigger ON monitor_data.table_size_data;
DROP FUNCTION IF EXISTS monitor_data.table_size_data_insert_trigger();

CREATE OR REPLACE FUNCTION monitor_data.table_size_data_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  l_year text;
  l_month text;
  l_partitioned_table_name text;
BEGIN
  l_month := to_char(NEW.tsd_timestamp, 'MM');
  l_year := to_char(NEW.tsd_timestamp, 'YYYY');
  l_partitioned_table_name := 'table_size_data_' || l_year || l_month;
  --RAISE WARNING 'l_partitioned_table_name: %', l_partitioned_table_name;

  IF NOT EXISTS (SELECT 1
                   FROM pg_tables
                  WHERE tablename = l_partitioned_table_name
                    AND schemaname = 'monitor_data')
  THEN
    PERFORM monitor_data.create_partitioned_tables(l_year, l_month, NEW.tsd_timestamp);
  END IF;

  EXECUTE 'INSERT INTO '|| l_partitioned_table_name ||' SELECT (' || quote_literal( NEW ) || '::monitor_data.'||l_partitioned_table_name || ').*' ;
  --RETURN NEW;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;




 --TODO a create a separate job for dropping old partition tables, once per month

DROP FUNCTION monitor_data.clean_up_partitioned_tables();

CREATE OR REPLACE FUNCTION monitor_data.clean_up_partitioned_tables()
  RETURNS VOID AS
$BODY$
DECLARE
  l_list_of_tables text[] := array['sproc_performance_data', 'table_io_data', 'table_size_data'];
  l_months_to_keep interval := '6 months';
  l_drop_string text;
  l_rec record;
BEGIN

  FOR l_rec IN (
    SELECT tablename, regexp_replace(tablename, E'\\D+','') as date
      FROM pg_tables
     WHERE schemaname = 'monitor_data'
       AND tablename ~ '(sproc_performance_data|table_io_data|table_size_data).*'
       AND regexp_replace(tablename, E'\\D+','') < to_char(now() - l_months_to_keep, 'YYYYMM')
       AND length(regexp_replace(tablename, E'\\D+','')) = 6
  )
  LOOP

    l_drop_string := 'DROP TABLE monitor_data.' || l_rec.tablename;
    RAISE WARNING 'dropping: %', l_rec.tablename;
    EXECUTE l_drop_string;

  END LOOP;

END;
$BODY$
LANGUAGE plpgsql;


  --sproc_performance_data|table_io_data|table_size_data

  ALTER TABLE sproc_performance_data RENAME TO sproc_performance_data_archive;
  CREATE TABLE sproc_performance_data ( LIKE sproc_performance_data_archive INCLUDING ALL );
  ALTER TABLE sproc_performance_data_archive INHERIT sproc_performance_data;


  ALTER TABLE table_io_data RENAME TO table_io_data_archive;
  CREATE TABLE table_io_data ( LIKE table_io_data_archive INCLUDING ALL );
  ALTER TABLE table_io_data_archive INHERIT table_io_data;


  ALTER TABLE table_size_data RENAME TO table_size_data_archive;
  CREATE TABLE table_size_data ( LIKE table_size_data_archive INCLUDING ALL );
  ALTER TABLE table_size_data_archive INHERIT table_size_data;

CREATE TRIGGER sproc_performance_data_insert_trigger
    BEFORE INSERT ON monitor_data.sproc_performance_data
    FOR EACH ROW EXECUTE PROCEDURE monitor_data.sproc_performance_data_insert_trigger();
CREATE TRIGGER table_size_data_insert_trigger
    BEFORE INSERT ON monitor_data.table_size_data
    FOR EACH ROW EXECUTE PROCEDURE monitor_data.table_size_data_insert_trigger();
CREATE TRIGGER table_io_data_insert_trigger
    BEFORE INSERT ON monitor_data.table_io_data
    FOR EACH ROW EXECUTE PROCEDURE monitor_data.table_io_data_insert_trigger();

COMMIT;

/*
DROP TRIGGER IF EXISTS sproc_performance_data_insert_trigger ON monitor_data.sproc_performance_data;
DROP TRIGGER IF EXISTS table_io_data_insert_trigger ON monitor_data.table_io_data;
DROP TRIGGER IF EXISTS table_size_data_insert_trigger ON monitor_data.table_size_data;
*/

/*
--later
ALTER TABLE monitor_data.sproc_performance_data_archive
    ADD CONSTRAINT date_range CHECK (sp_timestamp <= '013-07-04 00:00:00')
ALTER TABLE monitor_data.table_io_data_archive
    ADD CONSTRAINT date_range CHECK (tio_timestamp <= '013-07-04 00:00:00')
ALTER TABLE monitor_data.table_size_data_archive
    ADD CONSTRAINT date_range CHECK (tsd_timestamp <= '013-07-04 00:00:00')

*/


/* --rollback

BEGIN;

ALTER TABLE sproc_performance_data_archive NO INHERIT sproc_performance_data;
ALTER TABLE table_io_data_archive NO INHERIT table_io_data;
ALTER TABLE table_size_data_archive NO INHERIT table_size_data;

ALTER TABLE sproc_performance_data_201307 NO INHERIT sproc_performance_data;
ALTER TABLE table_io_data_archive_201307 NO INHERIT table_io_data;
ALTER TABLE table_size_data_archive_201307 NO INHERIT table_size_data;

ALTER TABLE sproc_performance_data RENAME TO sproc_performance_data_failed;
ALTER TABLE table_io_data_archive RENAME TO table_io_data_failed;
ALTER TABLE table_size_data_archive RENAME TO table_size_data_failed;

ALTER TABLE sproc_performance_data_archive RENAME TO sproc_performance_data;
ALTER TABLE table_io_data_archive RENAME TO table_io_data;
ALTER TABLE table_size_data_archive RENAME TO table_size_data;

COMMIT;

*/
