-- The key (the first argument) of the keyed SipHash functions must not be visible in formatted or logged queries.

-- { echoOn }
EXPLAIN SYNTAX SELECT sipHash64Keyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), number, 'visible_input') FROM numbers(1);
EXPLAIN SYNTAX SELECT sipHash128Keyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), number, 'visible_input') FROM numbers(1);
EXPLAIN SYNTAX SELECT sipHash128ReferenceKeyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), number, 'visible_input') FROM numbers(1);

-- A key which is not a literal is hidden as well.
EXPLAIN SYNTAX SELECT sipHash64Keyed((toUInt64(1311768467463790320), number + 18364758544493064720), 'visible_input') FROM numbers(1);

-- The functions without a key are not affected.
EXPLAIN SYNTAX SELECT sipHash64((1311768467463790320::UInt64, 18364758544493064720::UInt64), 'visible_input');
-- { echoOff }

-- Masking does not change the result.
SELECT sipHash64Keyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), 'visible_input') = sipHash64Keyed((materialize(1311768467463790320::UInt64), 18364758544493064720::UInt64), 'visible_input');

SELECT sipHash64Keyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), 'logged_input_1') FORMAT Null;
SELECT sipHash128Keyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), 'logged_input_2') FORMAT Null;
SELECT sipHash128ReferenceKeyed((1311768467463790320::UInt64, 18364758544493064720::UInt64), 'logged_input_3') FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT query
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%logged\_input\__%' AND query NOT LIKE '%system.query_log%'
ORDER BY query;

SELECT count()
FROM system.query_log
WHERE current_database = currentDatabase() AND query NOT LIKE '%system.query_log%'
    AND (query LIKE '%1311768467463790320%' OR query LIKE '%18364758544493064720%') AND query NOT LIKE '%sipHash64(%';
