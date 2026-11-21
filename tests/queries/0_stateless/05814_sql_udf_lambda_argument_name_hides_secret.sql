-- Tags: no-fasttest, no-parallel
-- no-fasttest: encrypt requires the OpenSSL-based functions, absent in the fast test build.
-- no-parallel: creates global SQL UDFs (CREATE FUNCTION); concurrent copies of this test would clash on the names.

-- A lambda argument of a SQL UDF call is named by its text: a secret in it is masked, and two different lambdas still
-- get different names.

SET format_display_secrets_in_show_and_select = 0;

DROP FUNCTION IF EXISTS map_05814;
DROP FUNCTION IF EXISTS untuple_map_05814;

CREATE FUNCTION map_05814 AS (fn, xs) -> arrayMap(fn, xs);
CREATE FUNCTION untuple_map_05814 AS (fn, xs) -> untuple(tuple(arrayMap(fn, xs)));

SELECT map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a'])
FORMAT TSVWithNames;
SELECT * FROM (SELECT map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a']))
FORMAT TSVWithNames;
SELECT * FROM (SELECT untuple_map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), untuple_map_05814(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a']))
FORMAT TSVWithNames;

-- A secret in the body of an `APPLY` lambda is masked too.
SELECT map_05814(x -> (SELECT * APPLY (v -> hex(encrypt('aes-128-ecb', 'a', 'SEKRIT_KEY_16BYT'))) FROM (SELECT 1 AS c)), [1])
FORMAT TSVWithNames;
SELECT * FROM (SELECT map_05814(x -> (SELECT * APPLY (v -> hex(encrypt('aes-128-ecb', 'a', 'SEKRIT_KEY_16BYT'))) FROM (SELECT 1 AS c)), [1]), map_05814(x -> (SELECT * APPLY (v -> hex(encrypt('aes-128-ecb', 'a', 'SEKRIT_KEY_16BYZ'))) FROM (SELECT 1 AS c)), [1]))
FORMAT TSVWithNames;

DROP FUNCTION map_05814;
DROP FUNCTION untuple_map_05814;
