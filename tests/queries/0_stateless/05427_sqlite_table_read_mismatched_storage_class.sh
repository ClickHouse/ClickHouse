#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column of an ordinary (non-STRICT) SQLite table can hold a value of any storage class, whatever its
# declared type. A value read into an integer column must be an integer, or the read fails: it must not be
# truncated, clamped or read as 0.

DB="${CLICKHOUSE_TMP}/05427.sqlite3"
rm -f "$DB"

sqlite3 "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, qty INTEGER, note TEXT);
INSERT INTO t VALUES (1, 10, 'int'), (2, 1.5, 'real'), (3, 2.9999, 'real'), (4, 'abc', 'text'),
    (5, '12', 'text that looks like an integer'), (6, x'01', 'blob'), (7, 1e30, 'real beyond int64');
CREATE TABLE ok(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO ok VALUES (1, 10), (2, '12'), (3, 3.0), (4, NULL), (5, -9223372036854775808);
CREATE TABLE bad(real_frac INTEGER, real_big INTEGER, text_word INTEGER, text_empty INTEGER, blob_cell INTEGER);
INSERT INTO bad VALUES (1.5, 1e30, 'abc', '', x'01');
CREATE TABLE other(r REAL, s TEXT);
INSERT INTO other VALUES (3, '12'), (-9223372036854775808.0, '-5');
CREATE TABLE edge(r REAL, s TEXT);
INSERT INTO edge VALUES (9223372036854775808.0, '99999999999999999999');
CREATE TABLE st(n INTEGER) STRICT;
INSERT INTO st VALUES (7);
CREATE TABLE tp(r REAL, frac REAL, b BLOB);
INSERT INTO tp VALUES (3, 1.5, x'3132');
CREATE TABLE tq(r REAL);
INSERT INTO tq VALUES (300);
"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- integer cells are read';
SELECT id, n FROM sqlite('${DB}', 'ok') ORDER BY id;
SELECT n FROM sqlite('${DB}', 'st');

SELECT '-- a REAL, TEXT or BLOB cell that is not a 64-bit integer fails the read';
SELECT real_frac FROM sqlite('${DB}', 'bad'); -- { serverError INCORRECT_DATA }
SELECT real_big FROM sqlite('${DB}', 'bad'); -- { serverError INCORRECT_DATA }
SELECT text_word FROM sqlite('${DB}', 'bad'); -- { serverError INCORRECT_DATA }
SELECT text_empty FROM sqlite('${DB}', 'bad'); -- { serverError INCORRECT_DATA }
SELECT blob_cell FROM sqlite('${DB}', 'bad'); -- { serverError INCORRECT_DATA }

SELECT '-- the same for the table function, the table engine, the database engine and CREATE TABLE AS SELECT';
SELECT id, qty FROM sqlite('${DB}', 't'); -- { serverError INCORRECT_DATA }
CREATE TABLE e ENGINE = SQLite('${DB}', 't');
SELECT qty FROM e; -- { serverError INCORRECT_DATA }
CREATE DATABASE lite ENGINE = SQLite('${DB}');
SELECT qty FROM lite.t; -- { serverError INCORRECT_DATA }
CREATE TABLE copy ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM lite.t; -- { serverError INCORRECT_DATA }

SELECT '-- a REAL or TEXT cell holding an integer exactly is read into an integer column';
CREATE TABLE other_int (r Int64, s Int32) ENGINE = SQLite('${DB}', 'other');
SELECT r, s FROM other_int ORDER BY r;
CREATE TABLE edge_r (r Int64) ENGINE = SQLite('${DB}', 'edge');
SELECT r FROM edge_r; -- { serverError INCORRECT_DATA }
CREATE TABLE edge_s (s Int64) ENGINE = SQLite('${DB}', 'edge');
SELECT s FROM edge_s; -- { serverError INCORRECT_DATA }

SELECT '-- the same for the integer types read as text';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tp_u64 (r UInt64, frac UInt64, b UInt64) ENGINE = SQLite('${DB}', 'tp');
CREATE TABLE tp_i128 (r Int128, frac Int128, b Int128) ENGINE = SQLite('${DB}', 'tp');
CREATE TABLE tp_lc (r LowCardinality(Int64), frac LowCardinality(Int64), b LowCardinality(Int64)) ENGINE = SQLite('${DB}', 'tp');
SELECT r FROM tp_u64;
SELECT r FROM tp_i128;
SELECT r FROM tp_lc;
SELECT frac FROM tp_u64; -- { serverError INCORRECT_DATA }
SELECT frac FROM tp_i128; -- { serverError INCORRECT_DATA }
SELECT frac FROM tp_lc; -- { serverError INCORRECT_DATA }
SELECT b FROM tp_u64; -- { serverError INCORRECT_DATA }
SELECT b FROM tp_i128; -- { serverError INCORRECT_DATA }
SELECT b FROM tp_lc; -- { serverError INCORRECT_DATA }

SELECT '-- a query-backed read of these types is unchanged';
CREATE TABLE tq_lc (r LowCardinality(UInt8)) ENGINE = SQLite('${DB}', query('SELECT r FROM tq'));
SELECT r FROM tq_lc; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }

SELECT '-- such values are read as String';
CREATE TABLE t_str (id Int64, qty Nullable(String)) ENGINE = SQLite('${DB}', 't');
SELECT id, qty FROM t_str WHERE id != 6 ORDER BY id;
SELECT id, qty FROM sqlite('${DB}', query('SELECT id, CAST(qty AS TEXT) AS qty FROM t WHERE id != 6')) ORDER BY id;
"

rm -f "$DB"
