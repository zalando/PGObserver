CREATE SCHEMA IF NOT EXISTS zz_utils;

GRANT USAGE ON SCHEMA zz_utils TO public;

/*
zz_utils.get_stat_statements() - a security workaround wrapper around pg_stat_statements view
*/


DO $OUTER$
DECLARE
  l_current_db text := current_database();
  l_sproc_text text := $SQL$
CREATE OR REPLACE FUNCTION zz_utils.get_stat_statements() RETURNS SETOF pg_stat_statements AS
$$
  select s.* from pg_stat_statements s join pg_database d on d.oid = s.dbid and d.datname = '%s'
$$ LANGUAGE sql VOLATILE SECURITY DEFINER;
$SQL$;
BEGIN
  PERFORM 1 from pg_views where viewname = 'pg_stat_statements';
  IF FOUND AND string_to_array( split_part(version(), ' ', 2), '.' )::int[] > ARRAY[9,1] THEN   --parameters normalized only from 9.2
    --RAISE WARNING '%', format(l_sproc_text, l_current_db);
    EXECUTE format(l_sproc_text, l_current_db);
    EXECUTE 'ALTER FUNCTION zz_utils.get_stat_statements() OWNER TO postgres';
    EXECUTE 'GRANT EXECUTE ON FUNCTION zz_utils.get_stat_statements() TO public';
  END IF;
END;
$OUTER$;