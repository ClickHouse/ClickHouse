-- A deferred build projects the bytes of its maps from the keys it stored. For string keys, it takes the bytes of all the
-- keys times the share of distinct rows. Here one short key fills 99% of the rows, and the other keys are long and
-- distinct. So the projection is far below what the build takes. With a spill threshold between the two, the deferred
-- build must switch to `GraceHashJoin` after its inserts, as the streaming build does after its last block.
--
-- The right side is one block: `numbers` reads it in one stream, and `max_block_size` is above its rows. So the checks
-- made before each block see nothing stored, and only the end of the build decides. With either layout the projection
-- is at most 41 MB and the build takes at least 60 MB.

SET join_algorithm = 'parallel_hash';
SET max_bytes_before_external_join = 50000000;
SET max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0; -- Otherwise the second run knows the size and does not defer.
SET max_threads = 4;
SET max_block_size = 1000000;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;

DROP VIEW IF EXISTS v_probe;
DROP VIEW IF EXISTS v_build;

CREATE VIEW v_probe AS SELECT concat(toString(number * 100), repeat('x', 10000)) AS k FROM numbers(100);
CREATE VIEW v_build AS SELECT if(number % 100 = 0, concat(toString(number), repeat('x', 10000)), 'a') AS k FROM numbers(200000);

SELECT count() FROM v_probe l INNER JOIN v_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, hash_join_layout = 'serial', log_comment = '05485_serial_deferred';
SELECT count() FROM v_probe l INNER JOIN v_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, hash_join_layout = 'serial', log_comment = '05485_serial_streaming';
SELECT count() FROM v_probe l INNER JOIN v_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, hash_join_layout = 'parallel', log_comment = '05485_parallel_deferred';
SELECT count() FROM v_probe l INNER JOIN v_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, hash_join_layout = 'parallel', log_comment = '05485_parallel_streaming';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinDeferredBuilds'], ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND log_comment LIKE '05485_%'
ORDER BY log_comment;

DROP VIEW v_probe;
DROP VIEW v_build;
