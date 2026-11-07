-- Row policy filter on a column that is a projection key but not in SELECT
-- caused NOT_FOUND_COLUMN_IN_BLOCK when aggregate projection optimization was applied

DROP TABLE IF EXISTS test_row_policy_proj;
DROP ROW POLICY IF EXISTS test_rp_proj ON test_row_policy_proj;

CREATE TABLE test_row_policy_proj
(
    tenant LowCardinality(String),
    sensor_id String,
    reading Float64,
    PROJECTION sensor_metadata
    (
        SELECT tenant, sensor_id, count(*)
        GROUP BY tenant, sensor_id
    )
)
ENGINE = MergeTree
PARTITION BY (tenant)
ORDER BY (sensor_id);

INSERT INTO test_row_policy_proj (tenant, sensor_id, reading) VALUES
    ('test', 'sensor_1', 100.0),
    ('test', 'sensor_2', 200.0);

CREATE ROW POLICY test_rp_proj ON test_row_policy_proj USING (tenant = 'test') TO all;

-- GROUP BY without aggregate functions (DistinctStep path)
SELECT sensor_id FROM test_row_policy_proj GROUP BY sensor_id ORDER BY sensor_id
    SETTINGS enable_analyzer = 1;

-- GROUP BY with aggregate function (AggregatingStep path)
SELECT sensor_id, count() FROM test_row_policy_proj GROUP BY sensor_id ORDER BY sensor_id
    SETTINGS enable_analyzer = 1;

DROP ROW POLICY test_rp_proj ON test_row_policy_proj;
DROP TABLE test_row_policy_proj;
