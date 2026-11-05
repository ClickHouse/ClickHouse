-- Regression for https://github.com/ClickHouse/ClickHouse/issues/102181
-- Parenthesized aliases are not valid in KILL WHERE predicates.
SELECT isNull(formatQueryOrNull('KILL QUERY WHERE (true AS x)'));
SELECT isNull(formatQueryOrNull('KILL QUERY WHERE ((true AS x))'));
SELECT isNull(formatQueryOrNull('KILL QUERY WHERE (true AS x) SYNC'));
SELECT isNull(formatQueryOrNull('KILL QUERY ON CLUSTER default WHERE (true AS x) TEST'));
SELECT isNull(formatQueryOrNull('KILL MUTATION WHERE (true AS x)'));
SELECT isNull(formatQueryOrNull('KILL TRANSACTION WHERE (true AS x)'));
SELECT isNull(formatQueryOrNull('KILL PART_MOVE_TO_SHARD WHERE (true AS x)'));

-- Parentheses and aliases inside subqueries remain valid.
SELECT isNotNull(formatQueryOrNull('KILL QUERY WHERE (true)'));
SELECT isNotNull(formatQueryOrNull('KILL MUTATION WHERE (true)'));
SELECT isNotNull(formatQueryOrNull('KILL TRANSACTION WHERE (true)'));
SELECT isNotNull(formatQueryOrNull('KILL PART_MOVE_TO_SHARD WHERE (true)'));
SELECT isNotNull(formatQueryOrNull('KILL QUERY WHERE query_id IN (SELECT query_id AS id FROM system.processes)'));
