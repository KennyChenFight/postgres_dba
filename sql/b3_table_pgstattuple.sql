--Table bloat (requires pgstattuple; expensive)

-- https://github.com/dataegret/pg-utils/tree/master/sql
-- pgstattuple extension required
-- WARNING: without table name/mask query will read all available tables which could cause I/O spikes

/*
 * This query analyzes table bloat using pgstattuple extension.
 * It provides detailed information about table space usage and waste.
 * 
 * Usage:
 * - To analyze all tables in all schemas: \set target_table '*' \set target_schema '*'
 * - To analyze all tables in a specific schema: \set target_table '*' \set target_schema 'schema_name'
 * - To analyze a specific table in all schemas: \set target_table 'table_name' \set target_schema '*'
 * - To analyze a specific table in a specific schema: \set target_table 'table_name' \set target_schema 'schema_name'
 * - To analyze tables matching a pattern: \set target_table 'table_pattern%' \set target_schema 'schema_name'
 */

\if `test :'table' = '*' && echo 1 || echo 0`
  \set show_all_tables 1
\else
  \set show_all_tables 0
\endif

\if `test :'schema' = '*' && echo 1 || echo 0`
  \set show_all_schemas 1
\else
  \set show_all_schemas 0
\endif

SELECT 
    nspname AS "Schema",
    relname AS "Table",
    pg_size_pretty(relation_size + toast_relation_size) AS "Total Size",
    pg_size_pretty(free_space) AS "Free Space",
    pg_size_pretty(toast_relation_size) AS "Toast Size",
    round(((relation_size - (relation_size - free_space)*100/fillfactor)*100/greatest(relation_size, 1))::numeric, 1) AS "Table Waste %",
    pg_size_pretty((relation_size - (relation_size - free_space)*100/fillfactor)::bigint) AS "Table Waste",
    round(((toast_free_space + relation_size - (relation_size - free_space)*100/fillfactor)*100/greatest(relation_size + toast_relation_size, 1))::numeric, 1) AS "Total Waste %",
    pg_size_pretty((toast_free_space + relation_size - (relation_size - free_space)*100/fillfactor)::bigint) AS "Total Waste"
FROM (
    SELECT 
        nspname, 
        relname,
        (SELECT free_space FROM pgstattuple(c.oid)) AS free_space,
        pg_relation_size(c.oid) AS relation_size,
        (CASE 
            WHEN reltoastrelid = 0 THEN 0 
            ELSE (SELECT free_space FROM pgstattuple(c.reltoastrelid)) 
         END) AS toast_free_space,
        COALESCE(pg_relation_size(c.reltoastrelid), 0) AS toast_relation_size,
        COALESCE((SELECT (regexp_matches(reloptions::text, E'.*fillfactor=(\\d+).*'))[1]), '100')::real AS fillfactor
    FROM 
        pg_class c
        LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE 
        nspname NOT IN ('pg_catalog', 'information_schema')
        AND nspname !~ '^pg_toast' 
        AND relkind = 'r'
        AND (
            (:show_all_schemas = 1 AND :show_all_tables = 1)
            OR (:show_all_schemas = 0 AND nspname = :'schema')
            OR (:show_all_tables = 0 AND relname = :'table')
            OR (nspname = :'schema' AND relname = :'table')
        )
) t
ORDER BY (toast_free_space + relation_size - (relation_size - free_space)*100/fillfactor) DESC
