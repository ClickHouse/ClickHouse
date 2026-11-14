-- A mutation that changes the column a previous rows TTL read, while the table has no rows TTL or a rows TTL
-- on another column, does not recalculate the stored rows-TTL bounds. It must drop their fingerprint, so that
-- a later `MODIFY TTL` back to a shift of the old expression rewrites the part instead of shifting bounds
-- computed from the old values (which would drop rows that expire far in the future).

SET alter_sync = 2, mutations_sync = 2;

-- The rows TTL is removed before the mutation.
DROP TABLE IF EXISTS t_ttl_mutation_removed;
CREATE TABLE t_ttl_mutation_removed (d DateTime('UTC'))
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_mutation_removed;
INSERT INTO t_ttl_mutation_removed SELECT now('UTC') - INTERVAL 100 DAY FROM numbers(10);
ALTER TABLE t_ttl_mutation_removed REMOVE TTL;
ALTER TABLE t_ttl_mutation_removed UPDATE d = now('UTC') + INTERVAL 1000 DAY WHERE 1;
ALTER TABLE t_ttl_mutation_removed MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_mutation_removed;

DROP TABLE t_ttl_mutation_removed;

-- The rows TTL is moved to another column without materialization before the mutation.
DROP TABLE IF EXISTS t_ttl_mutation_other_column;
CREATE TABLE t_ttl_mutation_other_column (d DateTime('UTC'), e DateTime('UTC'))
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_mutation_other_column;
INSERT INTO t_ttl_mutation_other_column SELECT now('UTC') - INTERVAL 100 DAY, now('UTC') FROM numbers(10);
ALTER TABLE t_ttl_mutation_other_column MODIFY TTL e + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_mutation_other_column UPDATE d = now('UTC') + INTERVAL 1000 DAY WHERE 1;
ALTER TABLE t_ttl_mutation_other_column MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_mutation_other_column;

DROP TABLE t_ttl_mutation_other_column;
