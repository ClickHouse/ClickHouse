SELECT SearchEngineID AS k1, count() AS c FROM test.hits GROUP BY k1 ORDER BY c DESC, k1 LIMIT 10
