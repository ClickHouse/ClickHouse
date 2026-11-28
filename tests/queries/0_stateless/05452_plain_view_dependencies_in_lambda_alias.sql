-- Regression test: an alias defined inside a lambda is visible only inside it, so the right argument of `IN` outside
-- the lambda with the same name refers to a table, which is a dependency of a plain view, while the right argument
-- of `IN` inside the lambda refers to the alias and is not a dependency.

CREATE TABLE ids (id UInt64) ENGINE = Memory;
INSERT INTO ids VALUES (1);

CREATE VIEW v AS SELECT arrayMap(x -> (x AS ids), [1]) AS unused, toUInt64(1) IN ids AS present;
CREATE VIEW v_inner AS SELECT arrayMap(x -> (x IN ([1, 2] AS ids)) AND (x IN ids), [1, 3]) AS present;

SELECT * FROM v;
SELECT * FROM v_inner;

SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'ids';

DROP TABLE ids SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE v;
DROP TABLE ids SETTINGS check_referential_table_dependencies = 1;
