-- The parser reads the format name of `INSERT ... FORMAT` as one identifier.
-- The formatted query must keep the quotes of a name that is not a plain word.
-- Otherwise the query is not parseable, or the remaining words of the name become the data of the query.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuerySingleLine('INSERT INTO t FORMAT `My Format`') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t FORMAT `a.b`') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t FROM INFILE \'x\' FORMAT `My Format`') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t SELECT * FROM input(\'a Int32\') FORMAT `My Format`') AS f, formatQuerySingleLine(f) = f;

-- The format of a plain word does not change, also when the word is a keyword.
SELECT formatQuerySingleLine('INSERT INTO t FORMAT TSV') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t FORMAT Null') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t FORMAT Values') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t VALUES') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('INSERT INTO t SELECT * FROM input(\'a Int32\') FORMAT Values') AS f, formatQuerySingleLine(f) = f;
