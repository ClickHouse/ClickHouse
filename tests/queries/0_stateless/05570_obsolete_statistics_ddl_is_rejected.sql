-- The obsolete `ALTER TABLE ... ADD/MODIFY STATISTICS` and the column-level `STATISTICS` clause are still parsed,
-- so that the entries stored by older versions (e.g. in the DDL log of a `Replicated` database) can be replayed,
-- but new submissions are rejected.

SELECT formatQuery('ALTER TABLE t ADD STATISTICS IF NOT EXISTS a, b TYPE tdigest, uniq');
SELECT formatQuery('ALTER TABLE t MODIFY STATISTICS a TYPE tdigest');
SELECT formatQuery('ALTER TABLE t DROP STATISTICS a');

DROP TABLE IF EXISTS t_obsolete_statistics_ddl;
CREATE TABLE t_obsolete_statistics_ddl (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_obsolete_statistics_ddl ADD STATISTICS a TYPE tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_obsolete_statistics_ddl MODIFY STATISTICS a TYPE tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_obsolete_statistics_ddl ADD COLUMN c UInt64 STATISTICS(tdigest); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_obsolete_statistics_ddl MODIFY COLUMN b UInt64 STATISTICS(tdigest); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_obsolete_statistics_ddl COMMENT COLUMN a 'x', ADD STATISTICS b TYPE tdigest; -- { serverError INCORRECT_QUERY }

DROP TABLE t_obsolete_statistics_ddl;
