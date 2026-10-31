-- A `MergeTree` modification hash must not return to its earlier value after a mutation is enqueued
-- and then cancelled with `KILL MUTATION`. Such a mutation may be applied on the fly by a read that
-- captured it in its mutations snapshot, while the parts, the metadata and the pending mutation counters
-- end up exactly as they were before. The query cache (`query_cache_use_only_when_data_was_not_changed`)
-- and `REFRESH ... IF CHANGED` compare the hash before and after the read, so a repeated hash would
-- certify the transient result. The interleaving itself is a race; this test checks the invariant that
-- forecloses it.

DROP TABLE IF EXISTS t_mutation_aba;
DROP TABLE IF EXISTS hashes_mutation_aba;

CREATE TABLE hashes_mutation_aba (k String, v Nullable(UInt128)) ENGINE = Memory;
CREATE TABLE t_mutation_aba (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mutation_aba VALUES (1);

SYSTEM STOP MERGES t_mutation_aba;

INSERT INTO hashes_mutation_aba SELECT 'before', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mutation_aba';

ALTER TABLE t_mutation_aba UPDATE x = 2 WHERE 1 SETTINGS mutations_sync = 0;

-- A pending mutation that a read may apply on the fly: the hash fails closed.
SELECT 'pending', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_mutation_aba';

KILL MUTATION WHERE database = currentDatabase() AND table = 't_mutation_aba' SYNC FORMAT Null;

INSERT INTO hashes_mutation_aba SELECT 'after', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mutation_aba';

SELECT 'data', x FROM t_mutation_aba;
SELECT 'both hashes known', countIf(v IS NOT NULL) FROM hashes_mutation_aba;
SELECT 'round trip changed the hash', (SELECT v FROM hashes_mutation_aba WHERE k = 'before') != (SELECT v FROM hashes_mutation_aba WHERE k = 'after');

DROP TABLE t_mutation_aba;
DROP TABLE hashes_mutation_aba;
