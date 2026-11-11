-- `dictGetKeys` must read the requested attribute of a DIRECT dictionary, whose read returns all source columns,
-- and must reject a REGEXP_TREE dictionary, whose read returns the tree nodes.

DROP DICTIONARY IF EXISTS dict_direct;
DROP DICTIONARY IF EXISTS dict_complex_direct;
DROP DICTIONARY IF EXISTS dict_regexp;
DROP TABLE IF EXISTS src_direct;
DROP TABLE IF EXISTS src_complex_direct;
DROP TABLE IF EXISTS src_regexp;

CREATE TABLE src_direct (id UInt64, a String, b String, n UInt64) ENGINE = Memory;
INSERT INTO src_direct VALUES (1, 'x', 'p', 10), (2, 'y', 'q', 20), (3, 'x', 'q', 30);
CREATE DICTIONARY dict_direct (id UInt64, a String, b String, n UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_direct')) LAYOUT(DIRECT());

SELECT arraySort(dictGetKeys('dict_direct', 'b', 'q')), arraySort(dictGetKeys('dict_direct', 'b', materialize('q')));
SELECT dictGetKeys('dict_direct', 'b', 'x'), dictGetKeys('dict_direct', 'b', materialize('x'));
SELECT dictGetKeys('dict_direct', 'n', 20), dictGetKeys('dict_direct', 'n', materialize(toUInt64(20)));

CREATE TABLE src_complex_direct (k1 UInt64, k2 String, a String, b String) ENGINE = Memory;
INSERT INTO src_complex_direct VALUES (1, 'u', 'x', 'p'), (2, 'v', 'y', 'q'), (3, 'w', 'x', 'q');
CREATE DICTIONARY dict_complex_direct (k1 UInt64, k2 String, a String, b String) PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'src_complex_direct')) LAYOUT(COMPLEX_KEY_DIRECT());

SELECT arraySort(dictGetKeys('dict_complex_direct', 'b', 'q')), arraySort(dictGetKeys('dict_complex_direct', 'b', materialize('q')));

CREATE TABLE src_regexp (id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)) ENGINE = Memory;
INSERT INTO src_regexp VALUES (1, 0, 'Android', ['name'], ['Android']);
CREATE DICTIONARY dict_regexp (regexp String, name String) PRIMARY KEY regexp
SOURCE(CLICKHOUSE(TABLE 'src_regexp')) LIFETIME(0) LAYOUT(REGEXP_TREE());

SELECT dictGetKeys('dict_regexp', 'name', 'Android'); -- { serverError UNSUPPORTED_METHOD }
SELECT dictGetKeys('dict_regexp', 'name', materialize('Android')); -- { serverError UNSUPPORTED_METHOD }

DROP DICTIONARY dict_direct;
DROP DICTIONARY dict_complex_direct;
DROP DICTIONARY dict_regexp;
DROP TABLE src_direct;
DROP TABLE src_complex_direct;
DROP TABLE src_regexp;
