-- Statistics are not declared per column anymore, they are collected according to the `auto_statistics_types` setting.
-- The metadata stored by older versions can still contain the `STATISTICS` clause of a column, and must be attachable.

DROP TABLE IF EXISTS t_attach_explicit_statistics;

ATTACH TABLE t_attach_explicit_statistics UUID '8c1d2c7f-5565-4b7a-9d3e-1f2a3b4c5d6e'
(
    a UInt64 STATISTICS(tdigest, uniq),
    b String STATISTICS(countmin)
)
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = 'basic';

SHOW CREATE TABLE t_attach_explicit_statistics FORMAT LineAsString;

SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' ORDER BY name;

INSERT INTO t_attach_explicit_statistics SELECT number, toString(number) FROM numbers(100);

SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

-- `DROP STATISTICS` was stored in the mutation entries by older versions. It is an obsolete synonym of `CLEAR STATISTICS`.
ALTER TABLE t_attach_explicit_statistics DROP STATISTICS a SETTINGS mutations_sync = 2;

SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics';
SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

ALTER TABLE t_attach_explicit_statistics MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

DROP TABLE t_attach_explicit_statistics;
