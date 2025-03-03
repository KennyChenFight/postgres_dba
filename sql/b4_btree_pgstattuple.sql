--B-tree indexes bloat (requires pgstattuple; expensive)

-- https://github.com/dataegret/pg-utils/tree/master/sql
-- pgstattuple extension required
-- WARNING: without index name/mask query will read all available indexes which could cause I/O spikes

/*
 * This query analyzes B-tree index bloat using pgstattuple extension.
 * It provides detailed information about index space usage and waste.
 * 
 * Usage:
 * - To analyze all indexes in all schemas: \set target_index '*' \set target_schema '*'
 * - To analyze all indexes in a specific schema: \set target_index '*' \set target_schema 'schema_name'
 * - To analyze a specific index in all schemas: \set target_index 'index_name' \set target_schema '*'
 * - To analyze a specific index in a specific schema: \set target_index 'index_name' \set target_schema 'schema_name'
 * - To analyze indexes matching a pattern: \set target_index 'index_pattern%' \set target_schema 'schema_name'
 */

\if `test :'index' = '*' && echo 1 || echo 0`
  \set show_all_indexes 1
\else
  \set show_all_indexes 0
\endif

\if `test :'schema' = '*' && echo 1 || echo 0`
  \set show_all_schemas 1
\else
  \set show_all_schemas 0
\endif

WITH index_data AS (
    SELECT
        schemaname AS schema_name,
        p.relname AS table_name,
        (SELECT spcname FROM pg_tablespace WHERE oid = c_table.reltablespace) AS table_tblspace,
        (SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace) AS index_tblspace,
        indexrelname AS index_name,
        (
            SELECT 
                (CASE 
                    WHEN avg_leaf_density = 'NaN' THEN 0
                    ELSE greatest(
                        ceil(
                            index_size * (1 - avg_leaf_density / (
                                COALESCE(
                                    (SELECT (regexp_matches(c.reloptions::text, E'.*fillfactor=(\\d+).*'))[1]),
                                    '90'
                                )::real
                            ))
                        )::bigint, 
                        0
                    ) 
                END)
            FROM pgstatindex(
                CASE 
                    WHEN p.indexrelid::regclass::text ~ '\.' 
                    THEN p.indexrelid::regclass::text 
                    ELSE schemaname || '.' || p.indexrelid::regclass::text 
                END
            )
        ) AS free_space,
        pg_relation_size(p.indexrelid) AS index_size,
        pg_relation_size(p.relid) AS table_size,
        idx_scan
    FROM 
        pg_stat_user_indexes p
        JOIN pg_class c ON p.indexrelid = c.oid
        JOIN pg_class c_table ON p.relid = c_table.oid
    WHERE
        pg_get_indexdef(p.indexrelid) LIKE '%USING btree%'
        AND (
            (:show_all_schemas = 1 AND :show_all_indexes = 1)
            OR (:show_all_schemas = 0 AND schemaname = :'schema')
            OR (:show_all_indexes = 0 AND indexrelname = :'index')
            OR (schemaname = :'schema' AND indexrelname = :'index')
        )
)
SELECT
    schema_name AS "Schema",
    table_name AS "Table",
    index_name AS "Index",
    pg_size_pretty(table_size) AS "Table Size",
    pg_size_pretty(index_size) AS "Index Size",
    idx_scan AS "Index Scans",
    CASE 
        WHEN index_size = 0 THEN 0
        ELSE round((free_space*100/index_size)::numeric, 1) 
    END AS "Wasted %",
    pg_size_pretty(free_space) AS "Wasted Space",
    COALESCE(table_tblspace, 'default') AS "Table Tablespace",
    COALESCE(index_tblspace, 'default') AS "Index Tablespace"
FROM 
    index_data
ORDER BY 
    free_space DESC
LIMIT 50;

