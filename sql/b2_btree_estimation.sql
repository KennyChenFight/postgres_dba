--B-tree index bloat (estimated)

-- Enhanced version of https://github.com/ioguix/pgsql-bloat-estimation/blob/master/btree/btree_bloat.sql

/*
* WARNING: executed with a non-superuser role, the query inspect only index on tables you are granted to read.
* WARNING: rows with unreliable statistics are marked with 'TRUE' in the "Unreliable Stats" column.
*          This happens when the index contains columns of type "name" which is not supported by statistics collector.
* This query is compatible with PostgreSQL 8.2+
*
* Usage:
* - To check all indexes: \set index '*' \set schema '*'
* - To check indexes for a specific schema: \set schema 'schema_name' \set index '*'
* - To check a specific index: \set index 'index_name' \set schema '*'
* - To check a specific index in a specific schema: \set index 'index_name' \set schema 'schema_name'
*/

\if `test :'schema' = '*' && echo 1 || echo 0`
  \set show_all_schemas 1
\else
  \set show_all_schemas 0
\endif

\if `test :'index' = '*' && echo 1 || echo 0`
  \set show_all_indexes 1
\else
  \set show_all_indexes 0
\endif

WITH index_metadata AS (
    -- Collect basic index metadata and statistics
    SELECT
        i.nspname AS schema_name,
        i.tblname AS table_name,
        i.idxname AS index_name,
        i.reltuples,
        i.relpages,
        i.relam,
        a.attrelid AS table_oid,
        CURRENT_SETTING('block_size')::NUMERIC AS bs,
        fillfactor,
        -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
        CASE 
            WHEN VERSION() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' THEN 8 
            ELSE 4 
        END AS maxalign,
        /* per page header, fixed size: 20 for 7.X, 24 for others */
        24 AS pagehdr,
        /* per page btree opaque data */
        16 AS pageopqdata,
        /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
        CASE
            WHEN MAX(COALESCE(s.null_frac, 0)) = 0 THEN 2 -- IndexTupleData size
            ELSE 2 + ((32 + 8 - 1) / 8) -- IndexTupleData size + IndexAttributeBitMapData size (max num field per index + 8 - 1) / 8
        END AS index_tuple_hdr_bm,
        /* data len: we remove null values save space using it fractional part from stats */
        SUM((1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 1024)) AS nulldatawidth,
        MAX(CASE WHEN a.atttypid = 'pg_catalog.name'::REGTYPE THEN 1 ELSE 0 END) > 0 AS is_na
    FROM 
        pg_attribute AS a
        JOIN (
            SELECT
                nspname, 
                tbl.relname AS tblname, 
                idx.relname AS idxname, 
                idx.reltuples, 
                idx.relpages, 
                idx.relam,
                indrelid, 
                indexrelid, 
                indkey::SMALLINT[] AS attnum,
                COALESCE(
                    SUBSTRING(
                        ARRAY_TO_STRING(idx.reloptions, ' ') 
                        FROM 'fillfactor=([0-9]+)'
                    )::SMALLINT, 
                    90
                ) AS fillfactor
            FROM 
                pg_index
                JOIN pg_class idx ON idx.oid = pg_index.indexrelid
                JOIN pg_class tbl ON tbl.oid = pg_index.indrelid
                JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
            WHERE 
                pg_index.indisvalid 
                AND tbl.relkind = 'r' 
                AND idx.relpages > 0
                -- Exclude system schemas
                AND nspname NOT IN ('pg_catalog', 'information_schema')
                AND (
                    (:show_all_schemas = 1 AND :show_all_indexes = 1)
                    OR (:show_all_schemas = 0 AND nspname = :'schema')
                    OR (:show_all_indexes = 0 AND idx.relname = :'index')
                    OR (nspname = :'schema' AND idx.relname = :'index')
                )
        ) AS i ON a.attrelid = i.indexrelid
        JOIN pg_stats AS s ON
            s.schemaname = i.nspname
            AND (
                (s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
                OR (s.tablename = i.idxname AND s.attname = a.attname) -- stats from functional cols
            )
        JOIN pg_type AS t ON a.atttypid = t.oid
    WHERE 
        a.attnum > 0
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9
), 
tuple_size_calculation AS (
    -- Calculate tuple size with alignment considerations
    SELECT
        *,
        (
            index_tuple_hdr_bm + maxalign
            -- Add padding to the index tuple header to align on MAXALIGN
            - CASE 
                WHEN index_tuple_hdr_bm % maxalign = 0 THEN maxalign 
                ELSE index_tuple_hdr_bm % maxalign 
              END
            + nulldatawidth + maxalign
            -- Add padding to the data to align on MAXALIGN
            - CASE
                WHEN nulldatawidth = 0 THEN 0
                WHEN nulldatawidth::INTEGER % maxalign = 0 THEN maxalign
                ELSE nulldatawidth::INTEGER % maxalign
              END
        )::NUMERIC AS nulldatahdrwidth
    FROM 
        index_metadata
), 
page_count_estimation AS (
    -- Estimate the number of pages needed for the index
    SELECT
        *,
        -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
        COALESCE(
            1 + CEIL(
                reltuples / FLOOR(
                    (bs - pageopqdata - pagehdr) / (4 + nulldatahdrwidth)::FLOAT
                )
            ), 
            0
        ) AS est_pages,
        COALESCE(
            1 + CEIL(
                reltuples / FLOOR(
                    (bs - pageopqdata - pagehdr) * fillfactor / (100 * (4 + nulldatahdrwidth)::FLOAT)
                )
            ), 
            0
        ) AS est_pages_ff
    FROM 
        tuple_size_calculation
        JOIN pg_am am ON tuple_size_calculation.relam = am.oid
    WHERE 
        am.amname = 'btree'
), 
bloat_calculation AS (
    -- Calculate bloat size and ratio
    SELECT
        *,
        bs * (relpages)::BIGINT AS real_size,
        bs * (relpages - est_pages)::BIGINT AS extra_size,
        100 * (relpages - est_pages)::FLOAT / NULLIF(relpages, 0) AS extra_ratio,
        bs * (relpages - est_pages_ff) AS bloat_size,
        100 * (relpages - est_pages_ff)::FLOAT / NULLIF(relpages, 0) AS bloat_ratio
    FROM 
        page_count_estimation
)
-- Format final output
SELECT
    -- Mark indexes with unreliable statistics (indexes with 'name' type columns)
    CASE is_na 
        WHEN TRUE THEN 'TRUE' 
        ELSE 'FALSE' 
    END AS "Unreliable Stats",
    schema_name || '.' || table_name AS "Table",
    LEFT(index_name, 50) || CASE WHEN LENGTH(index_name) > 50 THEN '…' ELSE '' END AS "Index",
    PG_SIZE_PRETTY(real_size::NUMERIC) AS "Size",
    CASE
        WHEN extra_size::NUMERIC >= 0
            THEN '~' || PG_SIZE_PRETTY(extra_size::NUMERIC)::TEXT || 
                 ' (' || ROUND(extra_ratio::NUMERIC, 2)::TEXT || '%)'
        ELSE NULL
    END AS "Extra",
    CASE
        WHEN bloat_size::NUMERIC >= 0
            THEN '~' || PG_SIZE_PRETTY(bloat_size::NUMERIC)::TEXT || 
                 ' (' || ROUND(bloat_ratio::NUMERIC, 2)::TEXT || '%)'
        ELSE NULL
    END AS "Bloat",
    CASE
        WHEN (real_size - bloat_size)::NUMERIC >= 0
            THEN '~' || PG_SIZE_PRETTY((real_size - bloat_size)::NUMERIC)
        ELSE NULL
    END AS "Live",
    fillfactor AS "Fillfactor"
FROM 
    bloat_calculation
ORDER BY 
    real_size DESC NULLS LAST;
