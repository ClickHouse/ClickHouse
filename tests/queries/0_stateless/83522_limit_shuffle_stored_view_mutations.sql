SET allow_experimental_shuffle_query = 1;
SET enable_lightweight_update = 1;
SET enable_lightweight_delete = 1;
SET allow_nondeterministic_mutations = 1;
SET mutations_sync = 2;
SET lightweight_delete_mode = 'alter_update';

CREATE TABLE shuffle_mutation_target (number UInt64, value UInt64)
ENGINE = MergeTree ORDER BY number
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO shuffle_mutation_target VALUES (0, 7);

CREATE VIEW shuffle_mutation_view SQL SECURITY DEFINER AS
SELECT number FROM numbers(1) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE VIEW shuffle_mutation_parameter AS
SELECT number FROM numbers({n:UInt64}) LIMIT 1 SHUFFLE;
CREATE VIEW shuffle_mutation_identifier AS SELECT number FROM {source:Identifier};
CREATE VIEW shuffle_mutation_deterministic AS SELECT (SELECT toUInt64(11)) AS number;

SET analyzer_inline_views = 1;
ALTER TABLE shuffle_mutation_target DELETE WHERE number IN (SELECT number FROM shuffle_mutation_view); -- { serverError SUPPORT_IS_DISABLED }
DELETE FROM shuffle_mutation_target WHERE number IN (SELECT number FROM shuffle_mutation_parameter(n = 1)); -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = 1 WHERE number IN (SELECT number FROM shuffle_mutation_identifier(source = 'shuffle_mutation_view')); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE shuffle_mutation_target UPDATE value = 1 WHERE number IN (SELECT number FROM shuffle_mutation_view); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE shuffle_mutation_target UPDATE value = (SELECT number FROM shuffle_mutation_view) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = (SELECT number FROM shuffle_mutation_parameter(n = 1)) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = (SELECT number FROM shuffle_mutation_identifier(source = 'shuffle_mutation_view')) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }

SET analyzer_inline_views = 0;
SET lightweight_delete_mode = 'lightweight_update_force';
ALTER TABLE shuffle_mutation_target DELETE WHERE number IN (SELECT number FROM shuffle_mutation_view); -- { serverError SUPPORT_IS_DISABLED }
DELETE FROM shuffle_mutation_target WHERE number IN (SELECT number FROM shuffle_mutation_parameter(n = 1)); -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = 1 WHERE number IN (SELECT number FROM shuffle_mutation_identifier(source = 'shuffle_mutation_view')); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE shuffle_mutation_target UPDATE value = 1 WHERE number IN (SELECT number FROM shuffle_mutation_view); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE shuffle_mutation_target UPDATE value = (SELECT number FROM shuffle_mutation_view) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = (SELECT number FROM shuffle_mutation_parameter(n = 1)) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
UPDATE shuffle_mutation_target SET value = (SELECT number FROM shuffle_mutation_identifier(source = 'shuffle_mutation_view')) WHERE 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT * FROM shuffle_mutation_target;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'shuffle_mutation_target';
SELECT number FROM shuffle_mutation_view;
SELECT number FROM shuffle_mutation_parameter(n = 1);
SELECT number FROM shuffle_mutation_identifier(source = 'shuffle_mutation_view');
SET analyzer_inline_views = 1;
ALTER TABLE shuffle_mutation_target UPDATE value = (SELECT number FROM shuffle_mutation_deterministic) WHERE number = 0;
SELECT * FROM shuffle_mutation_target;

DROP VIEW shuffle_mutation_identifier;
DROP VIEW shuffle_mutation_parameter;
DROP VIEW shuffle_mutation_view;
DROP VIEW shuffle_mutation_deterministic;
DROP TABLE shuffle_mutation_target;
