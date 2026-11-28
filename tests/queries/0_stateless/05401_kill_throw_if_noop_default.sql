SELECT 'Default';
KILL QUERY WHERE query_id = '05401_no_such_query'; -- { serverError NOTHING_TO_KILL }
KILL QUERY WHERE query_id = '05401_no_such_query' SYNC; -- { serverError NOTHING_TO_KILL }
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05401_no_such.txt'; -- { serverError NOTHING_TO_KILL }

SELECT 'Explicit opt-out';
KILL QUERY WHERE query_id = '05401_no_such_query' SETTINGS kill_throw_if_noop = 0;
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05401_no_such.txt' SETTINGS kill_throw_if_noop = 0;

SELECT 'Dry run';
KILL QUERY WHERE query_id = '05401_no_such_query' TEST;
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05401_no_such.txt' TEST;

SELECT 'Compatibility before 26.11';
SET compatibility = '26.10';
KILL QUERY WHERE query_id = '05401_no_such_query';
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05401_no_such.txt';

SELECT 'Compatibility since 26.11';
SET compatibility = '26.11';
KILL QUERY WHERE query_id = '05401_no_such_query'; -- { serverError NOTHING_TO_KILL }
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05401_no_such.txt'; -- { serverError NOTHING_TO_KILL }
SET compatibility = DEFAULT;
