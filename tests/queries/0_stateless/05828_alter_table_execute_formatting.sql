-- The parser reads the command name of `ALTER TABLE ... EXECUTE` as one identifier.
-- The formatted query must keep the quotes of a name that is not a plain identifier, otherwise it is not parseable.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE `my cmd`(`a b` = 1)') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE `a.b`()') AS f, formatQuerySingleLine(f) = f;

-- The format of a plain name does not change.
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2020-01-01 00:00:00\')') AS f, formatQuerySingleLine(f) = f;
