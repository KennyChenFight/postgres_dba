-- Table bloat (estimated)
-- This SQL is derived from https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql

/*
* WARNING: executed with a non-superuser role, the query inspect only tables you are granted to read.
* This query is compatible with PostgreSQL 9.0 and more
*/

\if `test :'schema' = '*' && echo 1 || echo 0`
  \set show_all_schemas 1
\else
  \set show_all_schemas 0
\endif

\if `test :'table' = '*' && echo 1 || echo 0`
  \set show_all_tables 1
\else
  \set show_all_tables 0
\endif

WITH table_metadata AS (
    -- 收集表的基本元數據和統計信息
    SELECT
        tbl.oid AS tblid,
        ns.nspname AS schema_name,
        tbl.relname AS table_name,
        tbl.reltuples,
        tbl.relpages AS heappages,
        COALESCE(toast.relpages, 0) AS toastpages,
        COALESCE(toast.reltuples, 0) AS toasttuples,
        COALESCE(
            SUBSTRING(
                ARRAY_TO_STRING(tbl.reloptions, ' ') 
                FROM '%fillfactor=#"__#"%' FOR '#'
            )::INT2, 
            100
        ) AS fillfactor,
        CURRENT_SETTING('block_size')::NUMERIC AS bs,
        CASE 
            WHEN VERSION() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' THEN 8 
            ELSE 4 
        END AS ma,
        24 AS page_hdr,
        23 + CASE 
                WHEN MAX(COALESCE(null_frac, 0)) > 0 THEN (7 + COUNT(*)) / 8 
                ELSE 0::INT 
             END
           + CASE 
                WHEN BOOL_OR(att.attname = 'oid' AND att.attnum < 0) THEN 4 
                ELSE 0 
             END AS tpl_hdr_size,
        SUM(
            (1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 1024)
        ) AS tpl_data_size,
        BOOL_OR(att.atttypid = 'pg_catalog.name'::REGTYPE)
            OR SUM(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> COUNT(s.attname) AS is_na
    FROM 
        pg_attribute AS att
        JOIN pg_class AS tbl 
            ON att.attrelid = tbl.oid 
            AND tbl.relkind = 'r'
        JOIN pg_namespace AS ns 
            ON ns.oid = tbl.relnamespace
        JOIN pg_stats AS s 
            ON s.schemaname = ns.nspname 
            AND s.tablename = tbl.relname 
            AND NOT s.inherited 
            AND s.attname = att.attname
        LEFT JOIN pg_class AS toast 
            ON tbl.reltoastrelid = toast.oid
    WHERE 
        NOT att.attisdropped 
        AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
        AND (
            (:show_all_schemas = 1 AND :show_all_tables = 1)
            OR (:show_all_schemas = 0 AND ns.nspname = :'schema')
            OR (:show_all_tables = 0 AND tbl.relname = :'table')
            OR (ns.nspname = :'schema' AND tbl.relname = :'table')
        )
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ORDER BY 
        2, 3
),
tuple_size_calculation AS (
    -- 計算元組大小和每個塊可存儲的數據量
    SELECT
        *,
        (
            4 + tpl_hdr_size + tpl_data_size + (2 * ma)
            - CASE 
                WHEN tpl_hdr_size % ma = 0 THEN ma 
                ELSE tpl_hdr_size % ma 
              END
            - CASE 
                WHEN CEIL(tpl_data_size)::INT % ma = 0 THEN ma 
                ELSE CEIL(tpl_data_size)::INT % ma 
              END
        ) AS tpl_size,
        bs - page_hdr AS size_per_block,
        (heappages + toastpages) AS tblpages
    FROM 
        table_metadata
),
page_count_estimation AS (
    -- 估計表實際需要的頁面數
    SELECT
        *,
        CEIL(reltuples / ((bs - page_hdr) / tpl_size)) + 
            CEIL(toasttuples / 4) AS est_tblpages,
        CEIL(reltuples / ((bs - page_hdr) * fillfactor / (tpl_size * 100))) + 
            CEIL(toasttuples / 4) AS est_tblpages_ff
    FROM 
        tuple_size_calculation
),
bloat_calculation AS (
    -- 計算膨脹大小和比率
    SELECT
        *,
        tblpages * bs AS real_size,
        (tblpages - est_tblpages) * bs AS extra_size,
        CASE 
            WHEN tblpages - est_tblpages > 0 
                THEN 100 * (tblpages - est_tblpages) / tblpages::FLOAT 
            ELSE 0 
        END AS extra_ratio,
        (tblpages - est_tblpages_ff) * bs AS bloat_size,
        CASE 
            WHEN tblpages - est_tblpages_ff > 0 
                THEN 100 * (tblpages - est_tblpages_ff) / tblpages::FLOAT 
            ELSE 0 
        END AS bloat_ratio
    FROM 
        page_count_estimation
        LEFT JOIN pg_stat_user_tables su 
            ON su.relid = tblid
)
-- 格式化最終輸出結果
SELECT
    -- Mark tables with unreliable statistics (tables with 'name' type columns or incomplete stats)
    CASE is_na 
        WHEN TRUE THEN 'TRUE' 
        ELSE 'FALSE' 
    END AS "Unreliable Stats",
    schema_name || '.' || table_name AS "Table",
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
    END AS "Bloat estimate",
    CASE
        WHEN (real_size - bloat_size)::NUMERIC >= 0
            THEN '~' || PG_SIZE_PRETTY((real_size - bloat_size)::NUMERIC)
        ELSE NULL
    END AS "Live",
    GREATEST(last_autovacuum, last_vacuum)::TIMESTAMP(0)::TEXT || 
        CASE GREATEST(last_autovacuum, last_vacuum)
            WHEN last_autovacuum THEN ' (auto)'
            ELSE '' 
        END AS "Last Vaccuum",
    (
        SELECT
            COALESCE(
                SUBSTRING(
                    ARRAY_TO_STRING(reloptions, ' ') 
                    FROM 'fillfactor=([0-9]+)'
                )::SMALLINT, 
                100
            )
        FROM 
            pg_class
        WHERE 
            oid = tblid
    ) AS "Fillfactor"
FROM 
    bloat_calculation
ORDER BY 
    bloat_size DESC NULLS LAST;
