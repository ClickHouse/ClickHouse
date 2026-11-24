-- `obfuscateQueryWithSeed` replaces identifiers with random words and literals with random
-- values while preserving the structure of the query, and a constant seed makes the result
-- deterministic: see the FunctionDocumentation of REGISTER_FUNCTION(obfuscateQuery) in
-- src/Functions/obfuscateQuery.cpp -- "Preserves the overall query structure and SQL syntax",
-- "Same seed always produces the same obfuscated result".
--
-- The following branches of `obfuscateQueries` (src/Parsers/obfuscateQueries.cpp:1173-1593)
-- were never entered by any test:
--   1259-1264  a string literal inside the data of INSERT ... FORMAT <name>
--   1266-1274  a backtick-quoted identifier inside that data, at most and above 32 bytes
--   1318-1325  the settings clause ends because the token after a value is not a comma
--   1328-1342  a setting name followed by a comma, and a setting name followed by a constraint
--   1381-1397  a dictionary key whose value is a bare word that is not a function call
--   1522-1535  a backtick-quoted identifier in a normal query position
--   1541-1575  a hex (x'...') or a binary (b'...') string literal
--
-- As in the existing tests of this function (03755, 04146, 05338, 05359) the assertions pin the
-- shape of the obfuscated query and never the pseudorandom words and digits: those change with
-- the dictionary and with the PRNG without any product regression. The patterns are written as
-- heredocs because they contain quotes.

-- Everything after FORMAT <name> is user data. The keywords, the format name, the quoting of the
-- string literal and of both quoted identifiers and the comma-separated shape survive, while
-- every user value is replaced. A quoted identifier of at most 32 bytes becomes a dictionary
-- word, a longer one becomes the decimal sipHash64 of its content.
WITH obfuscateQueryWithSeed($q$INSERT INTO t FORMAT Values ('secret text', `short name`, `an_identifier_longer_than_thirty_two_chars`, 42)$q$, 1) AS o
SELECT match(o, $re$^INSERT INTO [a-z_]+ FORMAT Values \('[a-z]+ [a-z]+', `[a-z]+`, `[0-9]+`, [0-9]+\)$$re$) AS insert_data_structure_kept,
       o NOT LIKE '%secret%' AND o NOT LIKE '%short name%' AND o NOT LIKE '%an_identifier%' AS insert_data_values_replaced;

-- Inside the data no word is recognised as a known identifier, so a literal that would keep its
-- leading `max` in a query position (`max` is a function name) is obfuscated as a whole.
SELECT obfuscateQueryWithSeed($q$INSERT INTO t FORMAT Values ('max_threads')$q$, 1) NOT LIKE '%max%' AS data_literal_keeps_no_known_word,
       startsWith(obfuscateQueryWithSeed($q$SELECT 'max_threads'$q$, 1), 'SELECT ''max_') AS query_literal_keeps_known_word;

-- The settings clause ends at the first token after a value that is not a comma: FORMAT and the
-- format name are kept as the keywords they are, and the column and the table are obfuscated.
WITH obfuscateQueryWithSeed($q$SELECT a FROM t SETTINGS max_threads = 8 FORMAT CSV$q$, 1) AS o
SELECT match(o, $re$^SELECT [a-z_]+ FROM [a-z_]+ SETTINGS max_threads = 8 FORMAT CSV$$re$) AS settings_clause_ends_before_format,
       o NOT LIKE 'SELECT a %' AS column_after_select_replaced;

-- A setting name without a value is followed by a comma and the next name follows.
SELECT obfuscateQueryWithSeed($q$SELECT 1 SETTINGS max_block_size, max_threads = 1$q$, 1)
         = 'SELECT 1 SETTINGS max_block_size, max_threads = 1' AS setting_names_and_comma_kept;

-- A setting name followed by a constraint keyword instead of `=` ends the clause; the name and
-- the constraints stay, the user name is obfuscated.
WITH obfuscateQueryWithSeed($q$CREATE USER alice SETTINGS max_threads MIN 1 MAX 8$q$, 1) AS o
SELECT match(o, $re$^CREATE USER [a-z_]+ SETTINGS max_threads MIN [0-9]+ MAX [0-9]+$$re$) AS setting_constraint_form_kept,
       o NOT LIKE '%alice%' AS user_name_replaced;

-- In a dictionary SOURCE block a key whose value is a bare word is resolved as soon as the next
-- token is not an opening bracket, so the structural keys HOST, PORT and TABLE stay while their
-- values are obfuscated.
WITH obfuscateQueryWithSeed($q$CREATE DICTIONARY dict (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST myhost PORT 9000 TABLE mytab)) LAYOUT(HASHED())$q$, 1) AS o
SELECT match(o, $re$^CREATE DICTIONARY [a-z_]+ \([a-z_]+ UInt64\) PRIMARY KEY [a-z_]+ SOURCE\(CLICKHOUSE\(HOST [a-z_]+ PORT [0-9]+ TABLE [a-z_]+\)\) LAYOUT\(HASHED\(\)\)$$re$) AS dictionary_structure_kept,
       o NOT LIKE '%myhost%' AND o NOT LIKE '%mytab%' AS dictionary_values_replaced;

-- A backtick-quoted identifier in a normal query position follows the same two rules as in the
-- data: the backticks stay, at most 32 bytes become a word and a longer one the decimal hash.
WITH obfuscateQueryWithSeed($q$SELECT `my column`, `another_identifier_longer_than_thirty_two` FROM t$q$, 1) AS o
SELECT match(o, $re$^SELECT `[a-z]+`, `[0-9]+` FROM [a-z_]+$$re$) AS quoted_identifier_structure_kept,
       o NOT LIKE '%my column%' AND o NOT LIKE '%another_identifier%' AS quoted_identifier_names_replaced;

-- A hex or a binary string literal keeps its prefix with its case, both quotes and the number of
-- digits, and every replacement digit stays inside the alphabet of the prefix. The hex table is
-- upper case, so the lower-case digit of X'0f' cannot survive being replaced.
WITH obfuscateQueryWithSeed($q$SELECT x'ABCD', X'0f', b'1010', B'11'$q$, 1) AS o
SELECT match(o, $re$^SELECT x'[0-9A-F]{4}', X'[0-9A-F]{2}', b'[01]{4}', B'[01]{2}'$$re$) AS hex_and_binary_literal_shape_kept,
       o NOT LIKE '%ABCD%' AS hex_literal_content_replaced;
