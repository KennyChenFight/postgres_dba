SELECT
  COALESCE(usename, '** ALL users **') AS "usename",
  COALESCE(datname, '** ALL databases **') AS "database",
  COALESCE(state, '** ALL states **') AS "current state",
  COUNT(*) AS "Count",
  COUNT(*) FILTER (WHERE state_change < NOW() - INTERVAL '1 minute') AS "state changed > 1m ago",
  COUNT(*) FILTER (WHERE state_change < NOW() - INTERVAL '1 hour') AS "state changed > 1h ago"
FROM pg_stat_activity
GROUP BY GROUPING SETS ((datname, usename, state), (usename, state), ())
ORDER BY
  usename IS NULL DESC,
  datname IS NULL DESC,
  2 ASC,
  3 ASC,
  COUNT(*) DESC
;