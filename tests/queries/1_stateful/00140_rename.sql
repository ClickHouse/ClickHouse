RENAME TABLE test.hits TO test.visits_tmp, test.visits TO test.hits, test.visits_tmp TO test.visits;

SELECT sum(Sign) FROM test.hits WHERE CounterID = 912887;
SELECT count() FROM test.visits WHERE CounterID = 732797;

RENAME TABLE test.hits TO test.hits_tmp, test.hits_tmp TO test.hits;

SELECT sum(Sign) FROM test.hits WHERE CounterID = 912887;
SELECT count() FROM test.visits WHERE CounterID = 732797;

RENAME TABLE test.hits TO test.visits_tmp, test.visits TO test.hits, test.visits_tmp TO test.visits;

SELECT count() FROM test.hits WHERE CounterID = 732797;
SELECT sum(Sign) FROM test.visits WHERE CounterID = 912887;

RENAME TABLE test.hits TO test.hits2, test.hits2 TO test.hits3, test.hits3 TO test.hits4, test.hits4 TO test.hits5, test.hits5 TO test.hits6, test.hits6 TO test.hits7, test.hits7 TO test.hits8, test.hits8 TO test.hits9, test.hits9 TO test.hits10;

SELECT count() FROM test.hits10 WHERE CounterID = 732797;

RENAME TABLE test.hits10 TO test.hits;

SELECT count() FROM test.hits WHERE CounterID = 732797;

RENAME TABLE test.hits TO default.hits, test.visits TO test.hits;

SELECT sum(Sign) FROM test.hits WHERE CounterID = 912887;
SELECT count() FROM default.hits WHERE CounterID = 732797;

RENAME TABLE test.hits TO test.visits, default.hits TO test.hits;

SELECT count() FROM test.hits WHERE CounterID = 732797;
SELECT sum(Sign) FROM test.visits WHERE CounterID = 912887;
