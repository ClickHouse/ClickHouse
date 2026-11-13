-- Tags: no-fasttest
-- Refuse creating vector similarity indexes on non-ordinary MergeTree tables unless a setting allows it

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_plain;
DROP TABLE IF EXISTS tab_allowed;

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab (id UInt64, sign Int8, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = CollapsingMergeTree(sign) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab (id UInt64, sign Int8, ver UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = SummingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = CoalescingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE tab_plain (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tab_allowed (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_vector_similarity_index_in_specialized_merge_tree = 1;

CREATE TABLE tab (id UInt64, vec Array(Float32)) ENGINE = ReplacingMergeTree ORDER BY id;
ALTER TABLE tab ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }
CREATE INDEX idx ON tab (vec) TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE tab MODIFY SETTING allow_vector_similarity_index_in_specialized_merge_tree = 1;
ALTER TABLE tab ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2);

ALTER TABLE tab RESET SETTING allow_vector_similarity_index_in_specialized_merge_tree;
DETACH TABLE tab;
ATTACH TABLE tab;
INSERT INTO tab VALUES (1, [1.0, 2.0]);
SELECT id FROM tab ORDER BY L2Distance(vec, [1.0, 2.0]) LIMIT 1;
ALTER TABLE tab ADD COLUMN c UInt8;
ALTER TABLE tab ADD INDEX idx2 vec TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE tab;
DROP TABLE tab_plain;
DROP TABLE tab_allowed;
