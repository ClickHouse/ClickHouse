-- Tags: memory-engine

CREATE TABLE stopped_actions_a (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_b (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_memory (x UInt64) ENGINE = Memory;

SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'tables' AND name = 'stopped_actions';
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name;

SYSTEM STOP MERGES stopped_actions_a;
SYSTEM STOP MERGES stopped_actions_a;
INSERT INTO stopped_actions_a VALUES (1);
INSERT INTO stopped_actions_a VALUES (2);
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_a';
-- Reading the registry must not release its lock.
OPTIMIZE TABLE stopped_actions_a FINAL; -- { serverError ABORTED }

-- Registration order does not affect display order. Unsupported actions do not create a lock.
SYSTEM STOP TTL MERGES stopped_actions_a;
SYSTEM STOP MOVES stopped_actions_a;
SYSTEM STOP CLEANUP stopped_actions_a;
SYSTEM STOP MOVES stopped_actions_b;
SYSTEM STOP MERGES stopped_actions_memory;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name SETTINGS max_block_size = 1;

-- Materialize the column when it is used only by a predicate.
SELECT name FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' AND has(stopped_actions, 'merges') ORDER BY name;
SYSTEM START MERGES stopped_actions_a;
SYSTEM START MERGES stopped_actions_a;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_a';
SYSTEM START TTL MERGES stopped_actions_a;
SYSTEM START MOVES stopped_actions_a;
SYSTEM START CLEANUP stopped_actions_a;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name;

SYSTEM STOP MERGES stopped_actions_a;
RENAME TABLE stopped_actions_a TO stopped_actions_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';
SYSTEM START MERGES stopped_actions_renamed;
SYSTEM START MOVES stopped_actions_b;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';

-- Temporary rows use a separate materialization path; check adjacent column alignment too.
CREATE TEMPORARY TABLE stopped_actions_temporary (x UInt64);
SELECT name, is_temporary, definer, stopped_actions FROM system.tables
WHERE is_temporary AND name = 'stopped_actions_temporary';
DROP TEMPORARY TABLE stopped_actions_temporary;

CREATE TABLE stopped_actions_distributed AS stopped_actions_b
ENGINE = Distributed(test_shard_localhost, currentDatabase(), stopped_actions_b);
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_distributed;
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_distributed;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name IN ('stopped_actions_b', 'stopped_actions_distributed') ORDER BY name;
SYSTEM START DISTRIBUTED SENDS stopped_actions_distributed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_distributed';

DROP TABLE stopped_actions_distributed;
DROP TABLE stopped_actions_renamed;
DROP TABLE stopped_actions_b;
DROP TABLE stopped_actions_memory;
