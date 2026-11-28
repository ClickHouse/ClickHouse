-- The parser reads each key of `SOURCE` and `LAYOUT`, the name of the source, and the name of the layout as one identifier.
-- The formatted query must keep the quotes of a name that is not a plain word, otherwise it is not parseable.
-- The second column shows that the formatted query is parseable and that the format is stable.

-- A key of the source.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(`k l` \'v\' `m.n` 1)) LAYOUT(FLAT()) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;
-- A key in a nested list of the source.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(a (`b c` \'v\' `d.e` 1))) LAYOUT(FLAT()) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;
-- The name of the source.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(`my src`(a \'v\')) LAYOUT(FLAT()) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;
-- A key of the layout.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(a \'v\')) LAYOUT(FLAT(`m n` 1)) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;
-- The name of the layout.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(a \'v\')) LAYOUT(`my layout`(a 1)) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;

-- The parser reads the name of a dictionary setting as a compound identifier.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(a \'v\')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`s t` = 1, `u.v` = 2, `w.` = 3)') AS f, formatQuerySingleLine(f) = f;

-- The format of a plain word does not change, also when the word is a keyword.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(TABLE \'t\' DB \'d\' WHERE \'x\' REPLICA (HOST \'h\' PRIORITY 1))) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(0) SETTINGS(max_threads = 1)') AS f, formatQuerySingleLine(f) = f;

-- A key from the AST JSON input does not go through the SQL parser. It can also contain a backquote and a backslash.
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(placeholder \'v\')) LAYOUT(FLAT()) LIFETIME(0)'), '"first":"placeholder"', '"first":"k l"')) AS f, formatQuerySingleLine(f) = f;
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(placeholder \'v\')) LAYOUT(FLAT()) LIFETIME(0)'), '"first":"placeholder"', '"first":"a`b\\\\c"')) AS f, formatQuerySingleLine(f) = f;

-- The server stores the formatted query as the definition of the dictionary. `ATTACH` parses the stored definition again.
-- The server hides the value of a secret key when it shows the definition. A quoted key has no effect on that.
CREATE DICTIONARY dict_05828 (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(TABLE 't' `k l` 'v' PASSWORD 'secret' REPLICA (HOST 'h' `m.n` 'w'))) LAYOUT(FLAT(`o p` 1)) LIFETIME(0) SETTINGS(`s.` = 1);
DETACH DICTIONARY dict_05828;
ATTACH DICTIONARY dict_05828;
SELECT extract(create_table_query, 'SOURCE.*') FROM system.tables WHERE database = currentDatabase() AND name = 'dict_05828';
DROP DICTIONARY dict_05828;
