-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/125928
-- `ipcrypt*` functions threw on the default empty string held by a `LowCardinality` dictionary
-- or behind a NULL, although no row contained it.

SET enable_ipcrypt = 1;
SET short_circuit_function_evaluation_for_nulls_threshold = 1;

SELECT 'LowCardinality';
SELECT ipcryptEncrypt(materialize(toLowCardinality('1.2.3.4')), '0123456789abcdef');
SELECT ipcryptDecrypt(materialize(toLowCardinality('9f4:e6e1:c77e:ffe8:49ac:6a6a:9f11:620f')), '0123456789abcdef');
SELECT ipcryptPrefixEncrypt(materialize(toLowCardinality('192.0.2.1')), '0123456789abcdeffedcba98765432101032547698badcfeefcdab8967452301');
SELECT ipcryptPrefixDecrypt(materialize(toLowCardinality('100.115.72.131')), '0123456789abcdeffedcba98765432101032547698badcfeefcdab8967452301');

DROP TABLE IF EXISTS t_ipcrypt_lc;
CREATE TABLE t_ipcrypt_lc (ip LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ipcrypt_lc VALUES ('1.2.3.4'), ('2001:db8::1');
SELECT ip, ipcryptDecrypt(ipcryptEncrypt(ip, '0123456789abcdef'), '0123456789abcdef') = ip FROM t_ipcrypt_lc ORDER BY ip;
DROP TABLE t_ipcrypt_lc;

-- The filtered out value stays in the dictionary.
DROP TABLE IF EXISTS t_ipcrypt_lc_filtered;
CREATE TABLE t_ipcrypt_lc_filtered (ip LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ipcrypt_lc_filtered VALUES ('1.2.3.4'), ('not an ip');
SELECT ipcryptEncrypt(ip, '0123456789abcdef') FROM t_ipcrypt_lc_filtered WHERE ip != 'not an ip';
DROP TABLE t_ipcrypt_lc_filtered;

SELECT 'Nullable';
SELECT ipcryptEncrypt(x, '0123456789abcdef') FROM (SELECT arrayJoin([NULL, '1.2.3.4'])::Nullable(String) AS x) ORDER BY ALL;
SELECT ipcryptDecrypt(x, '0123456789abcdef') FROM (SELECT arrayJoin([NULL, '9f4:e6e1:c77e:ffe8:49ac:6a6a:9f11:620f'])::LowCardinality(Nullable(String)) AS x) ORDER BY ALL;
SELECT ipcryptPrefixEncrypt(x, '0123456789abcdeffedcba98765432101032547698badcfeefcdab8967452301') FROM (SELECT arrayJoin([NULL, '192.0.2.1'])::Nullable(String) AS x) ORDER BY ALL;
SELECT ipcryptPrefixDecrypt(x, '0123456789abcdeffedcba98765432101032547698badcfeefcdab8967452301') FROM (SELECT arrayJoin([NULL, '100.115.72.131'])::LowCardinality(Nullable(String)) AS x) ORDER BY ALL;
-- A NULL may hide any value, not only ''.
SELECT ipcryptEncrypt(nullIf(x, 'garbage'), '0123456789abcdef') FROM (SELECT arrayJoin(['garbage', '1.2.3.4']) AS x) ORDER BY ALL;

SELECT 'an empty string in the data is still rejected';
SELECT ipcryptEncrypt(materialize(toLowCardinality('')), '0123456789abcdef'); -- { serverError BAD_ARGUMENTS }
SELECT ipcryptEncrypt(x, '0123456789abcdef') FROM (SELECT arrayJoin([NULL, ''])::Nullable(String) AS x); -- { serverError BAD_ARGUMENTS }
