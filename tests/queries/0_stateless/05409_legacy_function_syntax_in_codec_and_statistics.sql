-- Tags: no-random-merge-tree-settings
-- Random MergeTree settings make the client reformat every `CREATE ... MergeTree`, which
-- normalizes the legacy syntax before the server sees it.

-- Codec, statistics and index type names were `ASTFunction`s until 26.11, so stored metadata written
-- back then can spell them with anything a function call formatted: the parametric form `name(x)(y)`,
-- whose second list is the one the factories read, and the `NULLS` / `OVER` modifiers, which nothing
-- read. Such a definition must keep loading, with the name normalized on the way in.
SELECT formatQuerySingleLine($$CREATE TABLE t (x UInt64 STATISTICS(tdigest() IGNORE NULLS)) ENGINE = MergeTree ORDER BY x$$);
SELECT formatQuerySingleLine($$CREATE TABLE t (x UInt64 STATISTICS(uniq(1)(2))) ENGINE = MergeTree ORDER BY x$$);
SELECT formatQuerySingleLine($$CREATE TABLE t (x UInt64 CODEC(Delta(8) RESPECT NULLS, LZ4)) ENGINE = MergeTree ORDER BY x$$);
SELECT formatQuerySingleLine($$CREATE TABLE t (x UInt64, INDEX i x TYPE minmax() OVER (ORDER BY x) GRANULARITY 1) ENGINE = MergeTree ORDER BY x$$);

-- Loading such a definition is covered by `test_implicit_index_upgrade`, which writes it with an
-- older server; `ATTACH` here would need a UUID and an Atomic-only database.

-- A new definition is rejected instead.
CREATE TABLE t_new (x UInt64 STATISTICS(tdigest() IGNORE NULLS)) ENGINE = MergeTree ORDER BY x; -- { serverError SYNTAX_ERROR }
CREATE TABLE t_new (x UInt64 STATISTICS(uniq(1)(2))) ENGINE = MergeTree ORDER BY x; -- { serverError SYNTAX_ERROR }
CREATE TABLE t_new (x UInt64 CODEC(Delta(8) RESPECT NULLS, LZ4)) ENGINE = MergeTree ORDER BY x; -- { serverError SYNTAX_ERROR }
CREATE TABLE t_new (x UInt64, INDEX i x TYPE minmax() OVER (ORDER BY x) GRANULARITY 1) ENGINE = MergeTree ORDER BY x; -- { serverError SYNTAX_ERROR }

CREATE TABLE t_new (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_new MODIFY COLUMN x UInt64 CODEC(Delta(8) IGNORE NULLS, LZ4); -- { serverError SYNTAX_ERROR }
ALTER TABLE t_new ADD STATISTICS x TYPE tdigest() IGNORE NULLS; -- { serverError SYNTAX_ERROR }
ALTER TABLE t_new ADD INDEX i x TYPE minmax() IGNORE NULLS GRANULARITY 1; -- { serverError SYNTAX_ERROR }
CREATE INDEX j ON t_new x TYPE minmax() IGNORE NULLS GRANULARITY 1; -- { serverError SYNTAX_ERROR }

-- A codec-valued setting is checked wherever its value is new; a value already written in a stored
-- `SETTINGS` clause is not re-checked on startup, so it keeps loading.
ALTER TABLE t_new MODIFY SETTING default_compression_codec = 'Delta(8) IGNORE NULLS, LZ4'; -- { serverError SYNTAX_ERROR }

-- An empty parameter list is the canonical spelling of a name without parameters, not the legacy form.
ALTER TABLE t_new MODIFY COLUMN x UInt64 CODEC(LZ4());
SELECT countSubstrings(create_table_query, 'CODEC(LZ4)') FROM system.tables WHERE database = currentDatabase() AND name = 't_new';
DROP TABLE t_new;
