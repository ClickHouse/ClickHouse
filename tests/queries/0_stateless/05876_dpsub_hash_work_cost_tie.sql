-- The outer join graph of TPC-DS q54 at SF1, with the sizes and key NDVs of the benchmark's statistics: `r0` stands for
-- the `my_customers` subquery, which has no row estimate. Every join order then costs the same up to the last bits of
-- the sums, and the order chosen with `query_plan_join_order_use_interesting_orders` must be the one chosen without it.

SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_join_shard_by_pk_ranges = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;

CREATE TABLE r0 (c_customer_sk Int64, c_current_addr_sk Int64) ENGINE = MergeTree ORDER BY c_customer_sk SETTINGS auto_statistics_types = '';
CREATE TABLE ss (ss_item_sk Int64, ss_customer_sk Int64, ss_sold_date_sk Int64) ENGINE = MergeTree ORDER BY ss_item_sk SETTINGS auto_statistics_types = '';
CREATE TABLE ca (ca_address_sk Int64, ca_county String, ca_state String) ENGINE = MergeTree ORDER BY ca_address_sk SETTINGS auto_statistics_types = '';
CREATE TABLE st (s_store_sk Int64, s_county String, s_state String) ENGINE = MergeTree ORDER BY s_store_sk SETTINGS auto_statistics_types = '';
CREATE TABLE dd (d_date_sk Int64) ENGINE = MergeTree ORDER BY d_date_sk SETTINGS auto_statistics_types = '';

-- One row per table, so the reads are not optimized away; the sizes come from the hints.
INSERT INTO r0 VALUES (1, 1);
INSERT INTO ss VALUES (1, 1, 1);
INSERT INTO ca VALUES (1, 'C', 'S');
INSERT INTO st VALUES (1, 'C', 'S');
INSERT INTO dd VALUES (1);

SET param__internal_join_table_stat_hints = '{
    "r0": {},
    "ss": { "cardinality": 2880404, "distinct_keys": { "ss_customer_sk": 90423, "ss_sold_date_sk": 1784 } },
    "ca": { "cardinality": 50000, "distinct_keys": { "ca_address_sk": 49626, "ca_county": 1830, "ca_state": 51 } },
    "st": { "cardinality": 12, "distinct_keys": { "s_county": 1, "s_state": 1 } },
    "dd": { "cardinality": 61, "distinct_keys": { "d_date_sk": 61 } }
}';

SELECT replaceOne(explain, currentDatabase() || '.', '') FROM
(
    EXPLAIN
    SELECT count()
    FROM r0, ss, ca, st, dd
    WHERE c_customer_sk = ss_customer_sk AND c_current_addr_sk = ca_address_sk
        AND ca_county = s_county AND ca_state = s_state AND ss_sold_date_sk = d_date_sk
    SETTINGS query_plan_join_order_use_interesting_orders = 1
)
WHERE explain LIKE '%Join (%' OR explain LIKE '%ReadFromMergeTree%';

SELECT
(
    SELECT groupArray(explain) FROM
    (
        EXPLAIN
        SELECT count()
        FROM r0, ss, ca, st, dd
        WHERE c_customer_sk = ss_customer_sk AND c_current_addr_sk = ca_address_sk
            AND ca_county = s_county AND ca_state = s_state AND ss_sold_date_sk = d_date_sk
        SETTINGS query_plan_join_order_use_interesting_orders = 0
    )
) =
(
    SELECT groupArray(explain) FROM
    (
        EXPLAIN
        SELECT count()
        FROM r0, ss, ca, st, dd
        WHERE c_customer_sk = ss_customer_sk AND c_current_addr_sk = ca_address_sk
            AND ca_county = s_county AND ca_state = s_state AND ss_sold_date_sk = d_date_sk
        SETTINGS query_plan_join_order_use_interesting_orders = 1
    )
) AS same_plan_with_and_without_interesting_orders;

DROP TABLE r0;
DROP TABLE ss;
DROP TABLE ca;
DROP TABLE st;
DROP TABLE dd;
