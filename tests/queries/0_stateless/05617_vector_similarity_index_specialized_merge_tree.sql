-- Tags: no-fasttest
-- A vector similarity index is refused on MergeTree engines whose merges combine rows, unless a setting allows it.

DROP TABLE IF EXISTS t_05617;
DROP TABLE IF EXISTS t_05617_plain;
DROP TABLE IF EXISTS t_05617_allowed;

CREATE TABLE t_05617 (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_05617 (id UInt64, sign Int8, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = CollapsingMergeTree(sign) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_05617 (id UInt64, sign Int8, ver UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_05617 (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = SummingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_05617 (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_05617 (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = CoalescingMergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_05617_plain (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05617_allowed (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_vector_similarity_index_in_specialized_merge_tree = 1;

CREATE TABLE t_05617 (id UInt64, vec Array(Float32)) ENGINE = ReplacingMergeTree ORDER BY id;
ALTER TABLE t_05617 ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }
CREATE INDEX idx ON t_05617 (vec) TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_05617 MODIFY SETTING allow_vector_similarity_index_in_specialized_merge_tree = 1;
ALTER TABLE t_05617 ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2);

ALTER TABLE t_05617 RESET SETTING allow_vector_similarity_index_in_specialized_merge_tree;
DETACH TABLE t_05617;
ATTACH TABLE t_05617;
INSERT INTO t_05617 VALUES (1, [1.0, 2.0]);
SELECT id FROM t_05617 ORDER BY L2Distance(vec, [1.0, 2.0]) LIMIT 1;
ALTER TABLE t_05617 ADD COLUMN c UInt8;
ALTER TABLE t_05617 ADD INDEX idx2 vec TYPE vector_similarity('hnsw', 'L2Distance', 2); -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_05617;
DROP TABLE t_05617_plain;
DROP TABLE t_05617_allowed;
