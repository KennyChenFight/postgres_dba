-- 節點和當前數據庫信息：主/從節點、延遲、數據庫大小、臨時文件等

/*
對於 PostgreSQL 10 以前的版本，請先運行：

  \set postgres_dba_last_wal_receive_lsn pg_last_xlog_receive_location
  \set postgres_dba_last_wal_replay_lsn pg_last_xlog_replay_location
  \set postgres_dba_is_wal_replay_paused pg_is_xlog_replay_paused
*/

-- 檢查是否顯示所有數據庫
\if `test :'database' = '*' && echo 1 || echo 0`
  \set show_all_dbs 1
\else
  \set show_all_dbs 0
\endif

-- 1. 顯示通用系統信息（與特定數據庫無關）
WITH system_info AS (
  SELECT 'Postgres Version' AS metric, version() AS value
  
  UNION ALL
  
  SELECT 'Config file', (SELECT setting FROM pg_settings WHERE name = 'config_file')
  
  UNION ALL
  
  SELECT
    'Role',
    CASE WHEN pg_is_in_recovery() THEN 'Replica' || ' (delay: '
      || ((((CASE
          WHEN :postgres_dba_last_wal_receive_lsn() = :postgres_dba_last_wal_replay_lsn() THEN 0
          ELSE EXTRACT (epoch FROM NOW() - pg_last_xact_replay_timestamp())
        END)::INT)::TEXT || ' second')::INTERVAL)::TEXT
      || '; paused: ' || :postgres_dba_is_wal_replay_paused()::TEXT || ')'
    ELSE 'Master'
    END
  
  UNION ALL
  (
    WITH repl_groups AS (
      SELECT sync_state, state, STRING_AGG(HOST(client_addr), ', ') AS hosts
      FROM pg_stat_replication
      GROUP BY 1, 2
    )
    SELECT
      'Replicas',
      STRING_AGG(sync_state || '/' || state || ': ' || hosts, E'\n')
    FROM repl_groups
  )
  
  UNION ALL
 
  SELECT 'Started At', pg_postmaster_start_time()::TIMESTAMPTZ(0)::text
  
  UNION ALL
  
  SELECT 'Uptime', (NOW() - pg_postmaster_start_time())::INTERVAL(0)::TEXT
  
  UNION ALL
  
  SELECT 'Checkpoints', (SELECT (checkpoints_timed + checkpoints_req)::TEXT FROM pg_stat_bgwriter)
  
  UNION ALL
  
  SELECT
    'Forced Checkpoints',
    (
      SELECT ROUND(100.0 * checkpoints_req::NUMERIC /
        (NULLIF(checkpoints_timed + checkpoints_req, 0)), 1)::TEXT || '%'
      FROM pg_stat_bgwriter
    )
  
  UNION ALL
  
  SELECT
    'Checkpoint MB/sec',
    (
      SELECT ROUND((NULLIF(buffers_checkpoint::NUMERIC, 0) /
        ((1024.0 * 1024 /
          (current_setting('block_size')::NUMERIC))
            * EXTRACT('epoch' FROM NOW() - stats_reset)
        ))::NUMERIC, 6)::TEXT
      FROM pg_stat_bgwriter
    )
),
-- 2. 獲取要顯示的數據庫列表
db_list AS (
  SELECT datname
  FROM pg_database
  WHERE (:show_all_dbs = 1 AND datname NOT IN ('template0', 'template1'))
     OR (:show_all_dbs = 0 AND datname = :'database')
  ORDER BY datname
),
-- 3. 為每個數據庫生成完整的信息集
db_info AS (
  SELECT 
    d.datname,
    1 AS row_order,
    E'DATABASE: ' || d.datname AS metric,
    '' AS value
  FROM db_list d
  
  UNION ALL
  
  SELECT 
    d.datname,
    2 AS row_order,
    '  Size',
    pg_size_pretty(pg_database_size(d.datname))
  FROM db_list d
  
  UNION ALL
  
  SELECT 
    d.datname,
    3 AS row_order,
    '  Stats Since',
    CASE 
      WHEN s.stats_reset IS NULL THEN 'Never reset (collecting since database creation or server start)'
      ELSE s.stats_reset::TIMESTAMPTZ(0)::text
    END 
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    4 AS row_order,
    '  Stats Age',
    CASE 
      WHEN s.stats_reset IS NULL THEN 'N/A (statistics never explicitly reset)'
      ELSE (NOW() - s.stats_reset)::INTERVAL(0)::text 
    END 
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    5 AS row_order,
    '  Cache Effectiveness',
    (ROUND(s.blks_hit * 100::NUMERIC / NULLIF(s.blks_hit + s.blks_read, 0), 2))::TEXT || '%'
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    6 AS row_order,
    '  Successful Commits',
    (ROUND(s.xact_commit * 100::NUMERIC / NULLIF(s.xact_commit + s.xact_rollback, 0), 2))::TEXT || '%'
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    7 AS row_order,
    '  Conflicts',
    s.conflicts::TEXT
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    8 AS row_order,
    '  Temp Files: total size',
    pg_size_pretty(s.temp_bytes)::TEXT
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    9 AS row_order,
    '  Temp Files: total number',
    s.temp_files::TEXT
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    10 AS row_order,
    '  Temp Files: avg file size',
    COALESCE(pg_size_pretty(s.temp_bytes::NUMERIC / NULLIF(s.temp_files, 0)), '0 bytes')::TEXT
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    11 AS row_order,
    '  Deadlocks',
    s.deadlocks::TEXT
  FROM db_list d
  JOIN pg_stat_database s ON d.datname = s.datname
  
  UNION ALL
  
  SELECT 
    d.datname,
    12 AS row_order,
    '  Installed Extensions',
    (
      WITH exts AS (
        SELECT 
          extname || ' ' || extversion AS e, 
          (-1 + row_number() OVER (ORDER BY extname)) / 5 AS i
        FROM pg_extension
        WHERE :show_all_dbs = 0  -- 只在顯示單個數據庫時才顯示擴展信息
      ), 
      lines(l) AS (
        SELECT string_agg(e, ', ' ORDER BY i) AS l 
        FROM exts 
        GROUP BY i
      )
      SELECT string_agg(l, E'\n') FROM lines
    )
  FROM db_list d
  WHERE :show_all_dbs = 0 AND d.datname = :'database'
  
  UNION ALL
  
  SELECT 
    d.datname,
    13 AS row_order,
    REPEAT('-', 33),
    REPEAT('-', 88)
  FROM db_list d
),
all_info AS (
  -- 系統信息
  SELECT NULL::text AS datname, 0 AS row_order, metric, value FROM system_info
  UNION ALL
  -- 系統信息與數據庫信息之間的分隔線
  SELECT NULL::text, 0.5, REPEAT('-', 33), REPEAT('-', 88)
  UNION ALL
  -- 數據庫信息
  SELECT datname, row_order, metric, value FROM db_info
)
-- 5. 最終輸出，按照排序列進行排序
SELECT metric, value 
FROM all_info
ORDER BY 
  CASE WHEN datname IS NULL THEN 0 ELSE 1 END,  -- 先顯示系統信息
  datname NULLS FIRST,                          -- 然後按數據庫名稱排序
  row_order;  