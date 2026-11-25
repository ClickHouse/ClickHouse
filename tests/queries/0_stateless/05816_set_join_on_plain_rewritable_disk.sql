-- Tags: no-object-storage, no-replicated-database
-- Tag no-object-storage, no-replicated-database: as the other tests of the `local_plain_rewritable_03008` disk

-- The data directory of a new `Set` or `Join` table exists on a `plain_rewritable` disk only as the parent of its `tmp` directory.

CREATE TABLE t_set (a UInt64) ENGINE = Set SETTINGS disk = 'local_plain_rewritable_03008';
INSERT INTO t_set VALUES (1), (3);
SELECT number FROM numbers(5) WHERE number IN t_set ORDER BY number;
DETACH TABLE t_set;
ATTACH TABLE t_set;
SELECT number FROM numbers(5) WHERE number IN t_set ORDER BY number;
DROP TABLE t_set;

CREATE TABLE t_join (k UInt64, v String) ENGINE = Join(ANY, LEFT, k) SETTINGS disk = 'local_plain_rewritable_03008';
INSERT INTO t_join VALUES (1, 'a'), (2, 'b');
SELECT joinGet('t_join', 'v', toUInt64(2));
DETACH TABLE t_join;
ATTACH TABLE t_join;
SELECT joinGet('t_join', 'v', toUInt64(2));
DROP TABLE t_join;
