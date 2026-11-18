-- The tables that a materialized view joins with the inserted block are filtered by the join keys of the block
-- and are read by primary key, also with `FINAL`. See `query_plan_max_rows_in_block_to_filter_join_by_keys`.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS enriched;
DROP TABLE IF EXISTS people;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS regions;

CREATE TABLE people (id UInt64, company_id UInt64, name String, is_deleted UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE companies (id UInt64, name String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE regions (id UInt64, name String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;

-- Every tenth person has a newer version, which is deleted for odd keys.
SYSTEM STOP MERGES people;
INSERT INTO people SELECT number, number % 100, 'person' || toString(number), 0, 1 FROM numbers(20000);
INSERT INTO people SELECT number * 10, number % 100, 'renamed' || toString(number * 10), number % 2, 2 FROM numbers(2000);
INSERT INTO companies SELECT number, 'company' || toString(number) FROM numbers(1000);
INSERT INTO regions SELECT number, 'region' || toString(number) FROM numbers(20000);

CREATE TABLE events (id UInt64, person_id Nullable(UInt64), region_id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE enriched (id UInt64, person String, company String, region String) ENGINE = MergeTree ORDER BY id;

-- `people` and `regions` are joined by keys of the inserted block, `companies` by a key of `people`.
CREATE MATERIALIZED VIEW mv TO enriched AS
SELECT e.id AS id, p.name AS person, c.name AS company, r.name AS region
FROM events AS e
LEFT JOIN (SELECT id, company_id, name FROM people FINAL WHERE is_deleted = 0) AS p ON p.id = e.person_id
LEFT JOIN companies AS c ON c.id = p.company_id
LEFT JOIN regions AS r ON r.id = e.region_id;

INSERT INTO events SELECT number, if(number % 7 = 0, NULL, number * 35), number * 100 FROM numbers(60)
SETTINGS log_comment = 'with_filter';

SELECT * FROM enriched ORDER BY id LIMIT 8;
SELECT count(), sum(sipHash64(*)) FROM enriched;
TRUNCATE TABLE enriched;

INSERT INTO events SELECT number, if(number % 7 = 0, NULL, number * 35), number * 100 FROM numbers(60)
SETTINGS query_plan_max_rows_in_block_to_filter_join_by_keys = 0, log_comment = 'without_filter';

SELECT count(), sum(sipHash64(*)) FROM enriched;
TRUNCATE TABLE enriched;

INSERT INTO events SELECT number, if(number % 7 = 0, NULL, number * 35), number * 100 FROM numbers(60)
SETTINGS query_plan_max_rows_in_block_to_filter_join_by_keys = 59, log_comment = 'block_above_limit';

SELECT count(), sum(sipHash64(*)) FROM enriched;

SYSTEM FLUSH LOGS query_log;

SELECT
    sumIf(marks, log_comment = 'with_filter') * 2 < sumIf(marks, log_comment = 'without_filter') AS filter_selects_less_marks,
    sumIf(marks, log_comment = 'block_above_limit') = sumIf(marks, log_comment = 'without_filter') AS block_above_limit_is_not_used
FROM
(
    SELECT log_comment, ProfileEvents['SelectedMarks'] AS marks
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment IN ('with_filter', 'without_filter', 'block_above_limit')
);

DROP TABLE mv;
DROP TABLE events;
DROP TABLE enriched;
DROP TABLE people;
DROP TABLE companies;
DROP TABLE regions;
