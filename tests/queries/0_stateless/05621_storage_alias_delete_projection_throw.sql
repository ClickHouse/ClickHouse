-- DELETE through an Alias table must respect lightweight_mutation_projection_mode = 'throw' of the target.

DROP TABLE IF EXISTS t_alias_lwd_proj_alias;
DROP TABLE IF EXISTS t_alias_lwd_proj;

CREATE TABLE t_alias_lwd_proj (a UInt64, b UInt64, PROJECTION p (SELECT b ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS lightweight_mutation_projection_mode = 'throw';
INSERT INTO t_alias_lwd_proj VALUES (1, 10), (2, 20);
CREATE TABLE t_alias_lwd_proj_alias ENGINE = Alias(t_alias_lwd_proj);

DELETE FROM t_alias_lwd_proj WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }
DELETE FROM t_alias_lwd_proj_alias WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'rows', count() FROM t_alias_lwd_proj;
SELECT 'projection parts', count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_alias_lwd_proj' AND active;

DROP TABLE t_alias_lwd_proj_alias;
DROP TABLE t_alias_lwd_proj;
