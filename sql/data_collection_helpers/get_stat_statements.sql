CREATE SCHEMA IF NOT EXISTS zz_utils;

GRANT USAGE ON SCHEMA zz_utils TO public;

/*
zz_utils.get_stat_statements() - a security workaround wrapper around pg_stat_statements view

Be aware! Includes a security risk - non-superusers with execute grants on the sproc
will be able to see executed utility commands which might include "secret" data (e.g. alter role x with password y)!

Usage not really recommended for servers less than 9.2 (http://wiki.postgresql.org/wiki/What%27s_new_in_PostgreSQL_9.2#pg_stat_statements)
thus the "if" in code
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
    EXECUTE 'REVOKE EXECUTE ON FUNCTION zz_utils.get_stat_statements() FROM PUBLIC;';
    EXECUTE 'GRANT EXECUTE ON FUNCTION zz_utils.get_stat_statements() TO pgobserver_gatherer';
  END IF;
END;
$OUTER$;
