-- Regression test: the right argument of `IN` inside the expression of an alias of the same name refers to a table,
-- because an alias is not visible while its own expression is resolved, so it is a dependency of a plain view.

CREATE TABLE ids (id UInt64) ENGINE = Memory;
INSERT INTO ids VALUES (1);

CREATE VIEW v AS SELECT toUInt64(1) IN ids AS ids;
CREATE VIEW v_with AS WITH (toUInt64(1) IN ids) + 1 AS ids SELECT ids;

SELECT * FROM v;
SELECT * FROM v_with;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = 'ids';

DROP TABLE ids SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
