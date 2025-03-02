--Tables: table/index/TOAST size, number of rows
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

WITH table_info AS (
  SELECT
    c.oid,
    (SELECT spcname FROM pg_tablespace WHERE oid = reltablespace) AS tblspace,
    nspname AS schema_name,
    relname AS table_name,
    c.reltuples AS row_estimate,
    pg_total_relation_size(c.oid) AS total_bytes,
    pg_indexes_size(c.oid) AS index_bytes,
    pg_total_relation_size(reltoastrelid) AS toast_bytes,
    pg_total_relation_size(c.oid) - pg_indexes_size(c.oid) - COALESCE(pg_total_relation_size(reltoastrelid), 0) AS table_bytes
  FROM pg_class c
  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE relkind = 'r' AND nspname != 'pg_catalog'
  AND (
      (:show_all_schemas = 1 AND :show_all_tables = 1)
      OR (:show_all_schemas = 0 AND nspname = :'schema')
      OR (:show_all_tables = 0 AND relname = :'table')
  ) 
), 
all_info as (
  select
    null::oid as oid,
    null as tblspace,
    null as schema_name,
    '*** TOTAL ***' as table_name,
    sum(row_estimate) as row_estimate,
    sum(total_bytes) as total_bytes,
    sum(index_bytes) as index_bytes,
    sum(toast_bytes) as toast_bytes,
    sum(table_bytes) as table_bytes
  from table_info
  HAVING :show_all_schemas = 1 AND :show_all_tables = 1

  UNION ALL
  SELECT * FROM table_info
)
SELECT
  COALESCE(schema_name || '.', '') || table_name || COALESCE(' [' || NULLIF(tblspace, 'pg_default') || ']', '') AS "Table",
  '~' || CASE
    WHEN row_estimate > 10^12 THEN ROUND(row_estimate::numeric / 10^12::numeric, 0)::text || 'T'
    WHEN row_estimate > 10^9 THEN ROUND(row_estimate::numeric / 10^9::numeric, 0)::text || 'B'
    WHEN row_estimate > 10^6 THEN ROUND(row_estimate::numeric / 10^6::numeric, 0)::text || 'M'
    WHEN row_estimate > 10^3 THEN ROUND(row_estimate::numeric / 10^3::numeric, 0)::text || 'k'
    ELSE row_estimate::text
  END AS "Rows",
  pg_size_pretty(total_bytes) || ' (' || ROUND(
    100 * total_bytes::numeric / NULLIF(SUM(total_bytes) OVER (PARTITION BY (schema_name IS NULL), LEFT(table_name, 3) = '***'), 0),
    2
  )::text || '%)' AS "Total Size",
  pg_size_pretty(table_bytes) || ' (' || ROUND(
    100 * table_bytes::numeric / NULLIF(SUM(table_bytes) OVER (PARTITION BY (schema_name IS NULL), LEFT(table_name, 3) = '***'), 0),
    2
  )::text || '%)' AS "Table Size",
  pg_size_pretty(index_bytes) || ' (' || ROUND(
    100 * index_bytes::numeric / NULLIF(SUM(index_bytes) OVER (PARTITION BY (schema_name IS NULL), LEFT(table_name, 3) = '***'), 0),
    2
  )::text || '%)' AS "Index(es) Size",
  pg_size_pretty(toast_bytes) || ' (' || ROUND(
    100 * toast_bytes::numeric / NULLIF(SUM(toast_bytes) OVER (PARTITION BY (schema_name IS NULL), LEFT(table_name, 3) = '***'), 0),
    2
  )::text || '%)' AS "TOAST Size"
FROM all_info
WHERE schema_name != 'information_schema'
ORDER BY oid IS NULL DESC, total_bytes DESC NULLS LAST;
