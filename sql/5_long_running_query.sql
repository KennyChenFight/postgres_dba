\if `test :'state' = '*' && echo 1 || echo 0`
  \set show_all_states 1
\else
  \set show_all_states 0
\endif

\if `test :'usename' = '*' && echo 1 || echo 0`
  \set show_all_usenames 1
\else
  \set show_all_usenames 0
\endif

\if `test :'application_name' = '*' && echo 1 || echo 0`
  \set show_all_application_names 1
\else
  \set show_all_application_names 0
\endif

\if `test :'query' = '*' && echo 1 || echo 0`
  \set show_all_queries 1
\else
  \set show_all_queries 0
\endif


SELECT pid, now() - pg_stat_activity.query_start AS duration, usename, application_name, state, query, backend_type
FROM pg_stat_activity 
WHERE (now() - pg_stat_activity.query_start) >= interval :'interval'
AND (
    :show_all_states = 1
    OR state = :'state'
)
AND (
    :show_all_usenames = 1
    OR usename = :'usename'
)
AND (
    :show_all_application_names = 1
    OR application_name = :'application_name'
)
AND (
    :show_all_queries = 1
    OR query ILIKE '%' || :'query' || '%'
)
ORDER BY duration DESC;