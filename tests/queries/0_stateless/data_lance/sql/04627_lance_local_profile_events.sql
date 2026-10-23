-- ProfileEvents are selected by a unique log comment, so this test can run in parallel.
-- ProfileEvents are asserted via system.query_log; avoid concurrent interference.

SET log_queries = 1;

DROP TABLE IF EXISTS lance_local_profile_events;

CREATE TABLE lance_local_profile_events
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

-- Complete pushdown: a null-check is allowed for a nullable physical column.
SELECT id
FROM lance_local_profile_events
WHERE score IS NULL
FORMAT Null
SETTINGS log_comment = 'lance_profile_complete';

-- Partial AND: comparison is pushable, lower(name) is residual-only.
SELECT id
FROM lance_local_profile_events
WHERE score IS NULL AND lower(name) = 'b'
FORMAT Null
SETTINGS log_comment = 'lance_profile_partial';

-- LIMIT without ORDER BY so SourceStepWithFilter can pass limit into the scan.
SELECT id
FROM lance_local_profile_events
LIMIT 2
FORMAT Null
SETTINGS log_comment = 'lance_profile_limit';

SELECT count()
FROM lance_local_profile_events
FORMAT Null
SETTINGS log_comment = 'lance_profile_count';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['LancePlanScan'] > 0,
    ProfileEvents['LanceBatchesRead'] > 0,
    ProfileEvents['LanceRowsRead'] > 0,
    ProfileEvents['LanceLocalReadBytes'] > 0,
    ProfileEvents['LancePredicatePushdownComplete'] > 0,
    ProfileEvents['LanceProjectedColumns'] > 0,
    ProfileEvents['LanceArrowConvertMicroseconds'] >= 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_profile_complete'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT ProfileEvents['LancePredicatePushdownPartial'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_profile_partial'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT
    ProfileEvents['LanceLimitPushdown'] > 0,
    ProfileEvents['LanceRowsRead'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_profile_limit'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- count() may use the count fast path or a full scan depending on planner settings;
-- either path must account for rows.
SELECT
    ProfileEvents['LanceCountRows'] > 0 OR ProfileEvents['LanceRowsRead'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_profile_count'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lance_local_profile_events;
