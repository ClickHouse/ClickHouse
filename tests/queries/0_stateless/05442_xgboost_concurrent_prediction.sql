-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.
-- no-parallel-replicas: the dictionary exists only on the initiator, so a query spread over the
-- replicas fails with `Dictionary (model_05442_xgb) not found` on the others.

-- `predictXGBoost` runs on many threads at once against the same model, and every row gets the same
-- prediction as when the model is used by a single thread, for the `gbtree` and the `gblinear` boosters.

SET enable_xgboost = 1;
SET send_logs_level = 'error'; -- XGBoost warns that `gblinear` is deprecated

DROP DICTIONARY IF EXISTS model_05442_xgb;
DROP DICTIONARY IF EXISTS model_05442_linear;
DROP TABLE IF EXISTS training_05442;
DROP TABLE IF EXISTS predictions_05442;

CREATE TABLE training_05442 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05442 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(1000);

CREATE DICTIONARY model_05442_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05442'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY model_05442_xgb;

CREATE DICTIONARY model_05442_linear (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05442'))
LAYOUT(XGBOOST(num_iterations 10 booster 'gblinear'))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY model_05442_linear;

CREATE TABLE predictions_05442 (pass String, id UInt64, p Float64) ENGINE = MergeTree ORDER BY (pass, id);

-- `numbers_mt` with small blocks spreads the prediction over all the threads.
INSERT INTO predictions_05442 SELECT 'parallel', number, predictXGBoost('model_05442_xgb', number % 1000, intDiv(number, 97) % 150)
FROM numbers_mt(200000) SETTINGS max_threads = 16, max_block_size = 100, max_insert_threads = 16;
INSERT INTO predictions_05442 SELECT 'single', number, predictXGBoost('model_05442_xgb', number % 1000, intDiv(number, 97) % 150)
FROM numbers(200000) SETTINGS max_threads = 1, max_block_size = 100000;

SELECT count(), countIf(parallel.p = single.p)
FROM (SELECT id, p FROM predictions_05442 WHERE pass = 'parallel') AS parallel
INNER JOIN (SELECT id, p FROM predictions_05442 WHERE pass = 'single') AS single USING (id);

INSERT INTO predictions_05442 SELECT 'linear_parallel', number, predictXGBoost('model_05442_linear', number % 1000, intDiv(number, 97) % 150)
FROM numbers_mt(200000) SETTINGS max_threads = 16, max_block_size = 100, max_insert_threads = 16;
INSERT INTO predictions_05442 SELECT 'linear_single', number, predictXGBoost('model_05442_linear', number % 1000, intDiv(number, 97) % 150)
FROM numbers(200000) SETTINGS max_threads = 1, max_block_size = 100000;

SELECT count(), countIf(parallel.p = single.p)
FROM (SELECT id, p FROM predictions_05442 WHERE pass = 'linear_parallel') AS parallel
INNER JOIN (SELECT id, p FROM predictions_05442 WHERE pass = 'linear_single') AS single USING (id);

DROP DICTIONARY model_05442_xgb;
DROP DICTIONARY model_05442_linear;
DROP TABLE training_05442;
DROP TABLE predictions_05442;
