-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_read_settings;
CREATE TABLE t_substrait_read_settings (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_read_settings VALUES (1);

EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_read_settings SETTINGS apply_deleted_mask = 0; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_read_settings SETTINGS apply_mutations_on_fly = 1; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_read_settings SETTINGS apply_patch_parts = 0; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_read_settings;
