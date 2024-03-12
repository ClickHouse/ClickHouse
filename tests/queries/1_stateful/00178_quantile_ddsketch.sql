SELECT CounterID AS k, round(quantileDD(0.01, 0.5)(ResolutionWidth), 2) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, arrayMap(a -> round(a, 2), quantilesDD(0.01, 0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth)) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

SELECT CounterID AS k, round(quantileDD(0.01, 0.5)(ResolutionWidth), 2) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, arrayMap(a -> round(a, 2), quantilesDD(0.01, 0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth)) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
