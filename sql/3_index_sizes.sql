--Indexes: index size, number of rows
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

WITH index_info AS (
  SELECT
    i.indexrelid AS oid,
    (SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace) AS tblspace,
    n.nspname AS schema_name,
    c.relname AS index_name,
    pg_relation_size(i.indexrelid) AS index_bytes
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indexrelid  -- 索引關係
  JOIN pg_class t ON t.oid = i.indrelid    -- 表關係
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'i'  -- 只選擇索引
    AND n.nspname != 'pg_catalog'
    AND n.nspname != 'information_schema'
    AND n.nspname != 'pg_toast'
    AND (
      (:show_all_schemas = 1 AND :show_all_indexes = 1)
      OR (:show_all_schemas = 0 AND n.nspname = :'schema')
      OR (:show_all_indexes = 0 AND c.relname = :'index')
    )
), 
all_info AS (
  -- 總計行
  SELECT
    NULL::oid AS oid,
    NULL AS tblspace,
    NULL AS schema_name,
    '*** TOTAL ***' AS index_name,
    SUM(index_bytes) AS index_bytes
  FROM index_info
  WHERE :show_all_schemas = 1 AND :show_all_tables = 1 AND :show_all_indexes = 1

  UNION ALL
  
  SELECT * FROM index_info
)
SELECT
  COALESCE(schema_name || '.', '') || index_name || COALESCE(' [' || NULLIF(tblspace, 'pg_default') || ']', '') AS "Index",
  pg_size_pretty(index_bytes) || ' (' || ROUND(
    100 * index_bytes::numeric / NULLIF(SUM(index_bytes) OVER (PARTITION BY (schema_name IS NULL), LEFT(index_name, 3) = '***'), 0),
    2
  )::text || '%)' AS "Index Size"
FROM all_info
ORDER BY oid IS NULL DESC, index_bytes DESC NULLS LAST;
