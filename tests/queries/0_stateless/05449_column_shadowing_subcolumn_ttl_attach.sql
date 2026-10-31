-- A part that misses a column named like a subcolumn of another column (`a.size0` next to an `Array` column `a`) reads
-- that name as the subcolumn when a TTL expression reads it. `ALTER TABLE ... MODIFY TTL` refuses such a TTL while an
-- active part misses the column, and a detached part that misses it cannot be attached once such a TTL exists.

DROP TABLE IF EXISTS t_shadow_ttl_attach;
DROP TABLE IF EXISTS t_shadow_ttl_attach_src;

CREATE TABLE t_shadow_ttl_attach (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_ttl_attach VALUES ([]);
ALTER TABLE t_shadow_ttl_attach ADD COLUMN `a.size0` UInt64 DEFAULT 1;
SELECT a, `a.size0` FROM t_shadow_ttl_attach;

ALTER TABLE t_shadow_ttl_attach MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS materialize_ttl_after_modify = 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE t_shadow_ttl_attach DETACH PARTITION ID 'all';
ALTER TABLE t_shadow_ttl_attach MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_shadow_ttl_attach ATTACH PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_shadow_ttl_attach;

-- The same part, rewritten in a table without the TTL, holds the column and can be attached.
ALTER TABLE t_shadow_ttl_attach REMOVE TTL;
ALTER TABLE t_shadow_ttl_attach ATTACH PARTITION ID 'all';
OPTIMIZE TABLE t_shadow_ttl_attach FINAL;
ALTER TABLE t_shadow_ttl_attach DETACH PARTITION ID 'all';
ALTER TABLE t_shadow_ttl_attach MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_shadow_ttl_attach ATTACH PARTITION ID 'all';
OPTIMIZE TABLE t_shadow_ttl_attach FINAL;
SELECT a, `a.size0` FROM t_shadow_ttl_attach;

-- `ATTACH PARTITION FROM` another table that has no such TTL.
CREATE TABLE t_shadow_ttl_attach_src (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_ttl_attach_src VALUES ([]);
ALTER TABLE t_shadow_ttl_attach_src ADD COLUMN `a.size0` UInt64 DEFAULT 1;
ALTER TABLE t_shadow_ttl_attach ATTACH PARTITION ID 'all' FROM t_shadow_ttl_attach_src; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_shadow_ttl_attach;

DROP TABLE t_shadow_ttl_attach;
DROP TABLE t_shadow_ttl_attach_src;
