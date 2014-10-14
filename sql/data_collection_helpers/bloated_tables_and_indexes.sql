--CREATE SCHEMA IF NOT EXISTS zz_utils;

--GRANT USAGE ON SCHEMA zz_utils TO public;

/*
zz_utils.get_bloated_tables() - a security workaround wrapper around sql from https://wiki.postgresql.org/wiki/Show_database_bloat
*/

--drop function if exists zz_utils.get_bloated_tables(boolean, int);
DO $OUTER$
DECLARE
  l_current_db text := current_database();
  l_sproc_text text := $SQL$
    CREATE OR REPLACE FUNCTION zz_utils.get_bloated_tables(
        IN p_order_by_bloat_factor boolean DEFAULT FALSE,
        IN p_limit int DEFAULT 100,
        OUT schema_name text,
        OUT table_name text,
        OUT bloat_factor numeric,
        OUT wasted_bytes numeric,
        OUT wasted_bytes_pretty text)
     RETURNS SETOF record AS
    $$
    SELECT
      *,
      pg_size_pretty(wasted_bytes::int8) AS wasted_bytes_pretty
    FROM (
    SELECT
      schemaname::text AS schema_name,
      tablename::text AS table_name,
      ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS bloat_factor,
      CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wasted_bytes
    FROM
      (
        SELECT
          schemaname,
          tablename,
          cc.relpages,
          bs,
          CEIL((cc.reltuples*((datahdr+ma-
            (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float))
            AS otta
        FROM
          (
              SELECT
                ma,bs,schemaname,tablename,
                (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
                (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
              FROM
                (
                  SELECT
                    schemaname, tablename, hdr, ma, bs,
                    SUM((1-null_frac)*avg_width) AS datawidth,
                    MAX(null_frac) AS maxfracsum,
                    hdr+(
                        SELECT 1+count(*)/8
                        FROM pg_stats s2
                        WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
                      ) AS nullhdr
                  FROM
                    pg_stats s,
                    (
                      SELECT
                        (
                          SELECT current_setting('block_size')::numeric) AS bs,
                          CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
                      FROM (SELECT version() AS v) AS foo
                    ) AS constants
                  GROUP BY
                    1,2,3,4,5
              ) AS foo
          ) AS rs
          JOIN
          pg_class cc ON cc.relname = rs.tablename
          JOIN
          pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
      ) AS sml
    ) t
    ORDER BY
      CASE WHEN p_order_by_bloat_factor THEN bloat_factor ELSE wasted_bytes END DESC
    LIMIT
      p_limit
    $$ LANGUAGE sql VOLATILE SECURITY DEFINER;
    $SQL$;
BEGIN
    EXECUTE l_sproc_text;
    EXECUTE 'ALTER FUNCTION zz_utils.get_bloated_tables(boolean, int) OWNER TO postgres';
    EXECUTE 'GRANT EXECUTE ON FUNCTION zz_utils.get_bloated_tables(boolean, int) TO public';
END;
$OUTER$;


/*
zz_utils.get_bloated_indexes() - a security workaround wrapper around sql from https://wiki.postgresql.org/wiki/Show_database_bloat
*/
--drop function if exists zz_utils.get_bloated_indexes(boolean, int);

DO $OUTER$
DECLARE
  l_current_db text := current_database();
  l_sproc_text text := $SQL$
    CREATE OR REPLACE FUNCTION zz_utils.get_bloated_indexes(
        IN p_order_by_bloat_factor boolean DEFAULT FALSE,
        IN p_limit int DEFAULT 100,
        OUT schema_name text,
        OUT table_name text,
        OUT index_name text,
        OUT bloat_factor numeric,
        OUT wasted_bytes numeric,
        OUT wasted_bytes_pretty text)
     RETURNS SETOF record AS
    $$
    SELECT
      *,
      pg_size_pretty(wasted_bytes::int8) AS wasted_bytes_pretty
    FROM (
        SELECT
          schemaname::text AS schema_name,
          tablename::text AS table_name,
          iname::text AS index_name,
          ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat_factor,
          CASE WHEN ipages < iotta THEN 0 ELSE (bs*(ipages-iotta))::numeric END AS wasted_bytes
        FROM
          (
            SELECT
              schemaname,
              tablename,
              cc.relpages,
              bs,
              COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
              COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
            FROM
              (
                  SELECT
                    ma,bs,schemaname,tablename,
                    (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
                    (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
                  FROM
                    (
                      SELECT
                        schemaname, tablename, hdr, ma, bs,
                        SUM((1-null_frac)*avg_width) AS datawidth,
                        MAX(null_frac) AS maxfracsum,
                        hdr+(
                            SELECT 1+count(*)/8
                            FROM pg_stats s2
                            WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
                          ) AS nullhdr
                      FROM
                        pg_stats s,
                        (
                          SELECT
                            (SELECT current_setting('block_size')::numeric) AS bs,
                            CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                            CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
                          FROM (SELECT version() AS v) AS foo
                        ) AS constants
                      GROUP BY
                        1,2,3,4,5
                  ) AS foo
              ) AS rs
              JOIN
              pg_class cc ON cc.relname = rs.tablename
              JOIN
              pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
              LEFT JOIN
              pg_index i ON indrelid = cc.oid
              LEFT JOIN
              pg_class c2 ON c2.oid = i.indexrelid
          ) AS sml
    ) t
    ORDER BY
      CASE WHEN p_order_by_bloat_factor THEN bloat_factor ELSE wasted_bytes END DESC
    LIMIT
      p_limit
    $$ LANGUAGE sql VOLATILE SECURITY DEFINER;
    $SQL$;
BEGIN
    EXECUTE l_sproc_text;
    EXECUTE 'ALTER FUNCTION zz_utils.get_bloated_indexes(boolean, int) OWNER TO postgres';
    EXECUTE 'GRANT EXECUTE ON FUNCTION zz_utils.get_bloated_indexes(boolean, int) TO public';
END;
$OUTER$;