--Load profile

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
    s.relname AS table_name,
    s.schemaname AS schema_name,
    (SELECT spcname FROM pg_tablespace WHERE oid = reltablespace) AS tblspace,
    c.reltuples AS row_estimate,
    *,
    CASE WHEN n_tup_upd = 0 THEN NULL ELSE n_tup_hot_upd::numeric / n_tup_upd END AS upd_hot_ratio,
    n_tup_upd + n_tup_del + n_tup_ins AS mod_tup_total
  FROM pg_stat_user_tables s
  JOIN pg_class c ON c.oid = relid
  WHERE
  :show_all_schemas = 1 AND :show_all_tables = 1
  OR (:show_all_schemas = 0 AND s.schemaname = :'schema')
  OR (:show_all_tables = 0 AND s.relname = :'table')
), all_info AS (
  SELECT
    0 AS ord,
    '*** TOTAL ***' AS table_name,
    NULL AS schema_name,
    NULL AS tblspace,
    SUM(row_estimate) AS row_estimate,
    SUM(seq_tup_read) AS seq_tup_read,
    SUM(idx_tup_fetch) AS idx_tup_fetch,
    SUM(n_tup_ins) AS n_tup_ins,
    SUM(n_tup_del) AS n_tup_del,
    SUM(n_tup_upd) AS n_tup_upd,
    SUM(n_tup_hot_upd) AS n_tup_hot_upd,
    AVG(upd_hot_ratio) AS upd_hot_ratio,
    SUM(mod_tup_total) AS mod_tup_total
  FROM table_info
  HAVING :show_all_schemas = 1 AND :show_all_tables = 1

  UNION ALL

  SELECT
    1 AS ord,
    table_name,
    schema_name,
    tblspace,
    row_estimate,
    seq_tup_read,
    idx_tup_fetch,
    n_tup_ins, n_tup_del, n_tup_upd, n_tup_hot_upd, upd_hot_ratio, mod_tup_total
  FROM table_info
)
SELECT
  COALESCE(schema_name || '.', '') || table_name || COALESCE(' [' || NULLIF(tblspace, 'pg_default') || ']', '') AS "Table",
  '~' || CASE
    WHEN row_estimate > 10^12 THEN round(row_estimate::numeric / 10^12::numeric, 0)::text || 'T'
    WHEN row_estimate > 10^9 THEN round(row_estimate::numeric / 10^9::numeric, 0)::text || 'B'
    WHEN row_estimate > 10^6 THEN round(row_estimate::numeric / 10^6::numeric, 0)::text || 'M'
    WHEN row_estimate > 10^3 THEN round(row_estimate::numeric / 10^3::numeric, 0)::text || 'k'
    ELSE row_estimate::text
  END AS "Rows",
  CASE
    WHEN mod_tup_total = 0 THEN 'No writes'
    WHEN n_tup_ins::numeric / mod_tup_total > 0.7 THEN 'INSERT ~' || round(100 * n_tup_ins::numeric / mod_tup_total, 2)::text || '%'
    WHEN n_tup_upd::numeric / mod_tup_total > 0.7 THEN 'UPDATE ~' || round(100 * n_tup_upd::numeric / mod_tup_total, 2)::text || '%'
    WHEN n_tup_del::numeric / mod_tup_total > 0.7 THEN 'DELETE ~' || round(100 * n_tup_del::numeric / mod_tup_total, 2)::text || '%'
    ELSE 'Mixed: ' || 
      CASE WHEN n_tup_ins::numeric / mod_tup_total > 0.2 THEN 'I ~' || round(100 * n_tup_ins::numeric / mod_tup_total, 2)::text || '%' ELSE '' END ||
      CASE WHEN n_tup_upd::numeric / mod_tup_total > 0.2 THEN
        CASE WHEN n_tup_ins::numeric / mod_tup_total > 0.2 THEN ', ' ELSE '' END || 
        'U ~' || round(100 * n_tup_upd::numeric / mod_tup_total, 2)::text || '%' 
      ELSE '' END ||
      CASE WHEN n_tup_del::numeric / mod_tup_total > 0.2 THEN
        CASE WHEN (n_tup_ins::numeric / mod_tup_total > 0.2 OR n_tup_upd::numeric / mod_tup_total > 0.2) THEN ', ' ELSE '' END || 
        'D ~' || round(100 * n_tup_del::numeric / mod_tup_total, 2)::text || '%' 
      ELSE '' END
  END AS "Write Load Type",
  mod_tup_total AS "Tuples modified (I+U+D)",
  n_tup_ins AS "INSERTed",
  n_tup_del AS "DELETEd",
  n_tup_upd AS "UPDATEd",
  round(100 * upd_hot_ratio, 2) AS "HOT-updated, %",
  CASE WHEN seq_tup_read + coalesce(idx_tup_fetch, 0) > 0 THEN round(100 * seq_tup_read::numeric / (seq_tup_read + coalesce(idx_tup_fetch, 0)), 2) ELSE 0 END AS "SeqScan, %"
FROM all_info  
ORDER BY ord, row_estimate DESC;
