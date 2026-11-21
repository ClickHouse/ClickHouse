-- Tags: no-fasttest, no-ordinary-database
-- no-fasttest: the vector similarity index needs `usearch`, which the fast test build does not include
-- no-ordinary-database: an ATTACH with an explicit UUID needs an Atomic database
-- A full-definition ATTACH is rejected like CREATE when a vector similarity index is not allowed in a specialized MergeTree

SET database_replicated_allow_explicit_uuid = 3;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_vector_similarity_index_in_specialized_merge_tree = 0; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE tab UUID '7c3e9a52-4d1b-4f86-9a0e-5b2d8c61e3f7' (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_vector_similarity_index_in_specialized_merge_tree = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tab';
