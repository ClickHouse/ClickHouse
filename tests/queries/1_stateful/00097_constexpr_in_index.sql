-- Even in presence of OR, we evaluate the "0 IN (1, 2, 3)" as a constant expression therefore it does not prevent the index analysis.

SELECT count() FROM test.hits WHERE CounterID IN (14917930, 33034174) OR 0 IN (1, 2, 3) SETTINGS max_rows_to_read = 1000000, force_primary_key = 1;
