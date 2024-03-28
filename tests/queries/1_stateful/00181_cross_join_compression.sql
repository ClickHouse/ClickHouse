CREATE VIEW trunc_hits AS (SELECT * FROM test.hits LIMIT 1);

SELECT WatchID, CounterID, StartURL FROM trunc_hits, test.visits ORDER BY (WatchID, CounterID, StartURL) DESC LIMIT 1000;