-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and on mutations held on a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue

-- A pending CLEAR COLUMN is applied when a part is read, so it is refused while an unfinished mutation
-- that reads the column is ahead of it, whether it reads it directly, through a default, an ALIAS,
-- a TTL or a full rewrite of the part.

DROP TABLE IF EXISTS t_clear_mt;
DROP TABLE IF EXISTS t_clear_ttl;
DROP TABLE IF EXISTS t_clear_rmt;
DROP TABLE IF EXISTS t_clear_b;
DROP TABLE IF EXISTS t_clear_m;
DROP TABLE IF EXISTS t_clear_i;
DROP TABLE IF EXISTS t_clear_c;

CREATE TABLE t_clear_mt (id UInt32, a UInt32, b UInt32, w UInt32, tp Tuple(x UInt32), n Nested(x UInt32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_mt (id, a, b, tp, `n.x`) VALUES (1, 1111, 2222, tuple(5), [1, 2]), (2, 3333, 4444, tuple(6), [3]);
ALTER TABLE t_clear_mt
    ADD COLUMN d UInt32 DEFAULT b * 2,
    ADD COLUMN al UInt32 ALIAS b * 2,
    ADD COLUMN e UInt32 DEFAULT al + 1,
    ADD COLUMN f UInt32 DEFAULT tp.x * 2,
    ADD COLUMN g UInt32 DEFAULT arraySum(n.x),
    ADD COLUMN d2 UInt32 DEFAULT b + 1;

SYSTEM STOP MERGES t_clear_mt;
ALTER TABLE t_clear_mt MATERIALIZE COLUMN d SETTINGS mutations_sync = 0;
SELECT 'pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_clear_mt CLEAR COLUMN a SETTINGS alter_sync = 0;
SYSTEM START MERGES t_clear_mt;
ALTER TABLE t_clear_mt UPDATE w = w WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b, d FROM t_clear_mt ORDER BY id;

SYSTEM STOP MERGES t_clear_mt;

ALTER TABLE t_clear_mt UPDATE w = al WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt UPDATE w = w + 1 WHERE e > 0 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt DELETE WHERE e = 4445 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt MATERIALIZE COLUMN e SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

-- Writes the columns the part does not store from their defaults, so `e` from `b`.
ALTER TABLE t_clear_mt REWRITE PARTS SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt UPDATE w = w + 1 WHERE t_clear_mt.b > 0 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

-- An ALTER DELETE rewrites every column of a part it changes, so it writes `e` and `d2` from `b`.
ALTER TABLE t_clear_mt DELETE WHERE id = 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

-- A later ALTER must not make the pending reader read a column that a pending CLEAR clears.
ALTER TABLE t_clear_mt MATERIALIZE COLUMN d2 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN a SETTINGS alter_sync = 0;
ALTER TABLE t_clear_mt MODIFY COLUMN d2 UInt32 DEFAULT b + 2;
ALTER TABLE t_clear_mt MODIFY COLUMN d2 UInt32 DEFAULT a + 1; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt MATERIALIZE COLUMN f SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN tp SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_mt MATERIALIZE COLUMN g SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_mt CLEAR COLUMN n SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_mt' AND NOT is_done SYNC FORMAT Null;

SYSTEM START MERGES t_clear_mt;
ALTER TABLE t_clear_mt CLEAR COLUMN b SETTINGS alter_sync = 2, mutations_sync = 2;
SELECT id, b, d FROM t_clear_mt ORDER BY id;

CREATE TABLE t_clear_ttl (id UInt32, b UInt32, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_ttl VALUES (1, 2222, 1), (2, 4444, 2);
SYSTEM STOP MERGES t_clear_ttl;

ALTER TABLE t_clear_ttl MODIFY TTL toDateTime(if(b = 0, 4102444800, 946684800)) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_ttl MATERIALIZE TTL SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_clear_ttl CLEAR COLUMN w SETTINGS alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

-- A GROUP BY TTL aggregates every column, here `b` only in SET.
ALTER TABLE t_clear_ttl MODIFY TTL toDateTime(946684800) + toIntervalSecond(id) GROUP BY id SET w = sum(b) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_ttl MATERIALIZE TTL SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

-- Changing `w` re-evaluates the TTL, which reads `b`: an UPDATE of `w`, and a CLEAR of `w`.
ALTER TABLE t_clear_ttl MODIFY TTL toDateTime(if(b = 0, 4102444800, 946684800)) + toIntervalSecond(w) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_ttl UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

ALTER TABLE t_clear_ttl CLEAR COLUMN w SETTINGS alter_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

-- An UPDATE of a TTL input executes every TTL: a GROUP BY TTL aggregates `b`.
ALTER TABLE t_clear_ttl MODIFY TTL toDateTime(946684800) + toIntervalSecond(w) GROUP BY id SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_ttl UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

-- It also rewrites every column, so it writes the unstored `d` from `b`.
ALTER TABLE t_clear_ttl ADD COLUMN d UInt64 DEFAULT b * 2;
ALTER TABLE t_clear_ttl MODIFY TTL toDateTime(4102444800) + toIntervalSecond(w) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_ttl UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_ttl CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_ttl' AND NOT is_done SYNC FORMAT Null;

-- An UPDATE of `ts` executes every TTL, also the TTL of `z`, which reads `b` and not `ts`.
CREATE TABLE t_clear_c (id UInt32, ts DateTime, b UInt32, c UInt32 TTL ts, z UInt32 TTL toDateTime(if(b = 0, 4102444800, 946684800)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_c VALUES (1, '2100-01-01 00:00:00', 2222, 42, 99);
SYSTEM STOP MERGES t_clear_c;
ALTER TABLE t_clear_c UPDATE ts = ts + INTERVAL 1 SECOND WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_c CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_c' AND NOT is_done SYNC FORMAT Null;

CREATE TABLE t_clear_b (id UInt32, x UInt32, b UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_b VALUES (1, 1, 2222), (2, 2, 4444);
SYSTEM STOP MERGES t_clear_b;

-- A later ADD COLUMN must not make a pending full rewrite write a default from a pending-cleared column.
ALTER TABLE t_clear_b REWRITE PARTS SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_b CLEAR COLUMN b SETTINGS alter_sync = 0;
ALTER TABLE t_clear_b ADD COLUMN d1 UInt32 DEFAULT b * 2; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_b' AND NOT is_done SYNC FORMAT Null;

-- A compact part stores the index and projection columns it lacks, so these write `d` from `b`.
ALTER TABLE t_clear_b ADD COLUMN d UInt32 DEFAULT b * 2;
ALTER TABLE t_clear_b ADD INDEX idx_d d TYPE minmax GRANULARITY 1;
ALTER TABLE t_clear_b ADD PROJECTION p_d (SELECT id, d ORDER BY d);
ALTER TABLE t_clear_b MATERIALIZE INDEX idx_d SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_b CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_b' AND NOT is_done SYNC FORMAT Null;
ALTER TABLE t_clear_b MATERIALIZE PROJECTION p_d SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_b CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_b' AND NOT is_done SYNC FORMAT Null;
ALTER TABLE t_clear_b DROP INDEX idx_d, DROP PROJECTION p_d SETTINGS alter_sync = 0;

-- A later MODIFY TTL must not make a pending UPDATE re-evaluate a TTL over a pending-cleared column.
ALTER TABLE t_clear_b UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_b CLEAR COLUMN b SETTINGS alter_sync = 0;
ALTER TABLE t_clear_b MODIFY TTL toDateTime(if(b = 0, 4102444800, 946684800)) + toIntervalSecond(x) SETTINGS materialize_ttl_after_modify = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_b' AND NOT is_done SYNC FORMAT Null;

-- An UPDATE of `x` recomputes `m`, which re-evaluates a TTL over `m` that reads `b`.
ALTER TABLE t_clear_b ADD COLUMN m UInt32 MATERIALIZED x;
ALTER TABLE t_clear_b MODIFY TTL toDateTime(if(b = 0, 4102444800, 946684800)) + toIntervalSecond(m) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_clear_b UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_b CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_b' AND NOT is_done SYNC FORMAT Null;

-- An UPDATE of `x` recomputes `m`, which reads `b`.
CREATE TABLE t_clear_m (id UInt32, x UInt32, b UInt32, m UInt32 MATERIALIZED x + b) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_m (id, x, b) VALUES (1, 1, 2222), (2, 2, 4444);
SYSTEM STOP MERGES t_clear_m;
ALTER TABLE t_clear_m UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_m CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_m' AND NOT is_done SYNC FORMAT Null;

-- A mutation of a compact part stores the defaulted columns of its indexes and projections, so every mutation reads `d` and `b`, also after a later ADD INDEX or ADD PROJECTION.
CREATE TABLE t_clear_i (id UInt32, x UInt32, b UInt32, w UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_clear_i VALUES (1, 1, 2222, 0), (2, 2, 4444, 0);
ALTER TABLE t_clear_i ADD COLUMN d UInt32 DEFAULT b * 2;
SYSTEM STOP MERGES t_clear_i;
ALTER TABLE t_clear_i UPDATE w = w + 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_clear_i CLEAR COLUMN b SETTINGS alter_sync = 0;
ALTER TABLE t_clear_i ADD INDEX idx_d (x + d) TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_clear_i ADD PROJECTION p_d (SELECT id, x, d ORDER BY x); -- { serverError BAD_ARGUMENTS }
-- A hypothetical projection changes no mutation's reads.
CREATE HYPOTHETICAL PROJECTION p_h ON t_clear_i (SELECT id, x, d ORDER BY x);
DROP HYPOTHETICAL PROJECTION p_h ON t_clear_i;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_i' AND NOT is_done SYNC FORMAT Null;
ALTER TABLE t_clear_i ADD INDEX idx_d (x + d) TYPE minmax GRANULARITY 1;
ALTER TABLE t_clear_i UPDATE w = w + 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_clear_i CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_i' AND NOT is_done SYNC FORMAT Null;

CREATE TABLE t_clear_rmt (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_rmt', '1') ORDER BY id
SETTINGS merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;
INSERT INTO t_clear_rmt VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_clear_rmt ADD COLUMN d UInt32 DEFAULT b * 2 SETTINGS alter_sync = 2;
ALTER TABLE t_clear_rmt MODIFY SETTING max_replicated_mutations_in_queue = 0;
ALTER TABLE t_clear_rmt MATERIALIZE COLUMN d SETTINGS mutations_sync = 0;
SYSTEM SYNC REPLICA t_clear_rmt PULL;
SELECT 'pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_rmt' AND NOT is_done;
ALTER TABLE t_clear_rmt CLEAR COLUMN b SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_clear_rmt MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_clear_rmt UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b, d FROM t_clear_rmt ORDER BY id;
ALTER TABLE t_clear_rmt CLEAR COLUMN b SETTINGS alter_sync = 2, mutations_sync = 2;
SELECT id, b, d FROM t_clear_rmt ORDER BY id;

DROP TABLE t_clear_mt;
DROP TABLE t_clear_ttl;
DROP TABLE t_clear_rmt;
DROP TABLE t_clear_b;
DROP TABLE t_clear_m;
DROP TABLE t_clear_i;
DROP TABLE t_clear_c;
