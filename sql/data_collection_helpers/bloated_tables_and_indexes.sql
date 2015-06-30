CREATE SCHEMA IF NOT EXISTS zz_utils;

GRANT USAGE ON SCHEMA zz_utils TO public;

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
      schemaname::text,
      tblname::text,
      round(bloat_ratio::numeric, 1),
      bloat_size::numeric,
      pg_size_pretty(bloat_size::int8)
    FROM (
        SELECT current_database(), schemaname, tblname, bs*tblpages AS real_size,
        (tblpages-est_num_pages)*bs AS bloat_size,
        CASE WHEN tblpages - est_num_pages > 0
        THEN round((100 * (tblpages - est_num_pages)/tblpages::float)::numeric, 1)
        ELSE 0
        END AS bloat_ratio
        FROM (
        SELECT ceil(reltuples/((bs-page_hdr)/tpl_size)) + ceil(toasttuples/4) AS est_num_pages,
        tblpages, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
        FROM (
        SELECT
        ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
        - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
        - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
        ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
        toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, is_na
        FROM (
        SELECT
        tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
        tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
        coalesce(toast.reltuples, 0) AS toasttuples,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
        + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
        sum( (1-coalesce(s.null_frac, 0))
        * coalesce(
        CASE
        WHEN t.typlen = -1 THEN
        CASE WHEN s.avg_width < 127
        THEN s.avg_width + 1 ELSE s.avg_width + 4
        END
        WHEN t.typlen = -2 THEN s.avg_width + 1
        ELSE t.typlen
        END
        , 1024)) AS tpl_data_size,
        bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na
        FROM pg_attribute AS att
        JOIN pg_type AS t ON att.atttypid = t.oid
        JOIN pg_class AS tbl ON att.attrelid = tbl.oid
        JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
        JOIN pg_stats AS s ON s.schemaname=ns.nspname
        AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
        LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
        WHERE att.attnum > 0 AND NOT att.attisdropped
        AND tbl.relkind = 'r'
        GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
        ORDER BY 2,3
        ) AS s
        ) AS s2
        ) AS s3
        WHERE NOT is_na
    ) AS o
    ORDER BY
      CASE WHEN p_order_by_bloat_factor THEN bloat_ratio ELSE bloat_size END DESC
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
      schemaname::text,
      tblname::text,
      idxname::text,
      round(bloat_ratio::numeric, 1),
      bloat_size::numeric,
      pg_size_pretty(bloat_size::int8)
    FROM (
        SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(sub.relpages)::bigint AS real_size,
        bs*est_pages::bigint as estimated_size,
        bs*(sub.relpages-est_pages)::bigint AS bloat_size,
        100 * (sub.relpages-est_pages)::float / sub.relpages AS bloat_ratio, is_na
        FROM (
        SELECT bs, nspname, table_oid, tblname, idxname, relpages, coalesce(
        1+ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0
        ) AS est_pages, is_na
        FROM (
        SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid,
        ( index_tuple_hdr_bm +
        maxalign - CASE
        WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
        ELSE index_tuple_hdr_bm%maxalign
        END
        + nulldatawidth + maxalign - CASE
        WHEN nulldatawidth = 0 THEN 0
        WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
        ELSE nulldatawidth::integer%maxalign
        END
        )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
        FROM (
        SELECT
        i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,
        current_setting('block_size')::numeric AS bs,
        CASE
        WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
        ELSE 4
        END AS maxalign,
        24 AS pagehdr,
        16 AS pageopqdata,
        CASE WHEN max(coalesce(s.null_frac,0)) = 0
        THEN 2
        ELSE 2 + (( 32 + 8 - 1 ) / 8)
        END AS index_tuple_hdr_bm,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
        max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
        FROM pg_attribute AS a
        JOIN (
        SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,
        indrelid, indexrelid, indkey::smallint[] AS attnum
        FROM pg_index
        JOIN pg_class idx ON idx.oid=pg_index.indexrelid
        JOIN pg_class tbl ON tbl.oid=pg_index.indrelid
        JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
        WHERE pg_index.indisvalid AND tbl.relkind = 'r'
        ) AS i ON a.attrelid = i.indexrelid
        JOIN pg_stats AS s ON s.schemaname = i.nspname
        AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE))
        OR (s.tablename = i.idxname AND s.attname = a.attname))
        JOIN pg_type AS t ON a.atttypid = t.oid
        WHERE a.attnum > 0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ) AS s1
        ) AS s2
        JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = 'btree'
        ) AS sub
        WHERE NOT is_na
    ) AS o
    ORDER BY
      CASE WHEN p_order_by_bloat_factor THEN bloat_ratio ELSE bloat_size END DESC
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