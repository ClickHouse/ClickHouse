-- Tags: distributed

-- A hash join on a single `String` key packs the keys (see `enable_packed_string_keys_in_hash_join`) or compares them exactly.
-- The keys are packed only when the hash table with exact keys is larger than the L2 cache. A key of more than 15 bytes is hashed
-- for every left row, so then the right side also needs `min_rows_for_packed_string_keys_in_hash_join` rows, unless the join has a
-- runtime filter that passes mostly the rows with a match, which an `INNER` join gets and a `LEFT` join does not. A key without the
-- `basic` statistics has an unknown length, which counts as long. A minimum of 0 packs every join.
-- The cache size differs between machines, so the right sides are far from it: 1000 rows take 32 to 65 KB, 1000000 rows 32 to 65 MB.
-- A join with packed keys counts the keys it inserts in `JoinPackedStringKeysInline` and `JoinPackedStringKeysHashed`,
-- a join with exact keys counts none.
-- The choice is made on the host that runs the join. A shard of a `Distributed` table plans the join itself, from its own statistics.

SET enable_parallel_replicas = 0;
SET enable_packed_string_keys_in_hash_join = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'hash';
-- Settings that decide whether the join gets a runtime filter. The left side is small, so there is no minimum.
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- The tables with the suffix `_plain` have the same keys without statistics. `auto_statistics_types` is empty, so the
-- statistics do not depend on the server configuration.
CREATE TABLE short_small_stats (k String STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE short_small_plain (k String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE short_big_stats (k String STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE short_big_plain (k String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE long_small_stats (k String STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE long_small_plain (k String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE long_big_stats (k String STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE long_big_plain (k String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE long_big_nullable_stats (k Nullable(String) STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO short_small_stats SELECT toString(number) FROM numbers(1000);
INSERT INTO short_small_plain SELECT k FROM short_small_stats;
INSERT INTO short_big_stats SELECT toString(number) FROM numbers(1000000);
INSERT INTO short_big_plain SELECT k FROM short_big_stats;
INSERT INTO long_small_stats SELECT concat('a key longer than 15 bytes ', toString(number)) FROM numbers(1000);
INSERT INTO long_small_plain SELECT k FROM long_small_stats;
INSERT INTO long_big_stats SELECT concat('a key longer than 15 bytes ', toString(number)) FROM numbers(1000000);
INSERT INTO long_big_plain SELECT k FROM long_big_stats;
INSERT INTO long_big_nullable_stats SELECT k FROM long_big_stats;

-- A small hash table: exact keys, unless the minimum is 0.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:short_small_stats:default';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:short_small_stats:min_rows_0';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_plain AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:short_small_plain:default';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_plain AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:short_small_plain:min_rows_0';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:long_small_stats:default';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_small_stats:min_rows_0';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_small_plain AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:long_small_plain:default';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_small_plain AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_small_plain:min_rows_0';

-- A large hash table: packed keys of known short length. Keys of unknown or long length need the minimum or a runtime filter.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_plain:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:short_big_plain:default';
-- The statistics tell that the keys are short, so a `LEFT` join packs them. Without them the length is unknown and counts as long.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN short_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:left:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_plain:left:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:short_big_plain:left:default';
-- Long keys of a `LEFT` join, and of a join without the filter, stay exact below the minimum.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:long_big_stats:inner:default';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:inner:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:left:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, enable_join_runtime_filters = 0, log_comment = '05814:long_big_stats:no_filter:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_big_stats:left:min_rows_0';
-- A `LEFT SEMI` join keeps the rows that can match and has the filter. The filter of a `LEFT ANTI` join removes the rows with a match,
-- so the rows left all miss, and it does not count.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT SEMI JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:left_semi:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT ANTI JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:left_anti:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT ANTI JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_big_stats:left_anti:min_rows_0';
-- A `Nullable(String)` key is packed like a `String` key.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_nullable_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_nullable_stats:inner:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_nullable_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_big_nullable_stats:inner:min_rows_0';

-- Where the key has statistics: a subquery that passes the column on keeps them. The result of an expression in `ON` has none, also when the
-- expression returns the type of its argument and the statistics of the column would apply to its width, as `hex` doubles it.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN (SELECT k FROM short_big_stats) AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:subquery:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_big_stats AS r ON l.k = concat(r.k, 'x')
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:expression:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN short_big_stats AS r ON l.k = concat(r.k, 'x')
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:expression:left:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN short_big_stats AS r ON l.k = hex(r.k)
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:hex:left:min_rows_large';

-- Without an estimate of the optimizer, the right side is as large as its own read is estimated to be.
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_plain AS r ON l.k = r.k
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = '05814:short_small_plain:no_estimate';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = '05814:short_small_stats:no_estimate';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN short_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:short_big_stats:no_estimate:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:inner:no_estimate:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l LEFT JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_stats:left:no_estimate:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:long_big_stats:inner:min_rows_0';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:long_big_plain:min_rows_large';
SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10)) AS l INNER JOIN long_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '05814:long_big_plain:default';

-- The shards read the right table themselves.
CREATE TABLE l_local (k String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE l_dist AS l_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), l_local);
INSERT INTO l_local SELECT toString(number) FROM numbers(10);

-- Each shard decides for itself, from its own statistics, its own estimate of the right side and its own runtime filter. It packs
-- the keys of the large table, unless they are long or of unknown length and it has no runtime filter below the minimum, and
-- keeps exact keys for the small table unless the minimum is 0.
SELECT count() FROM l_dist AS l INNER JOIN short_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:shards:short_big_stats:min_rows_large';
SELECT count() FROM l_dist AS l INNER JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:shards:short_big_plain:min_rows_large';
SELECT count() FROM l_dist AS l INNER JOIN short_big_plain AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:shards:short_big_plain:min_rows_0';
SELECT count() FROM l_dist AS l INNER JOIN short_small_stats AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:shards:short_small_stats:min_rows_0';
SELECT count() FROM l_dist AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:shards:long_big_stats:inner:min_rows_large';
SELECT count() FROM l_dist AS l LEFT JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 100000000, log_comment = '05814:shards:long_big_stats:left:min_rows_large';
SELECT count() FROM l_dist AS l INNER JOIN long_big_stats AS r ON l.k = r.k
FORMAT Null SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05814:shards:long_big_stats:inner:min_rows_0';

SYSTEM FLUSH LOGS query_log;

SELECT replaceOne(log_comment, '05814:', '') AS config,
    ProfileEvents['JoinPackedStringKeysInline'] > 0 AS inline_keys, ProfileEvents['JoinPackedStringKeysHashed'] > 0 AS hashed_keys
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND startsWith(log_comment, '05814:') AND NOT startsWith(log_comment, '05814:shards:')
ORDER BY event_time_microseconds;

-- The queries of the shards: at least one, and the keys their joins inserted packed.
SELECT replaceOne(log_comment, '05814:shards:', '') AS config, count() > 0,
    sum(ProfileEvents['JoinPackedStringKeysInline']) > 0 AS inline_keys, sum(ProfileEvents['JoinPackedStringKeysHashed']) > 0 AS hashed_keys
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND startsWith(log_comment, '05814:shards:'))
GROUP BY log_comment
ORDER BY min(event_time_microseconds);
