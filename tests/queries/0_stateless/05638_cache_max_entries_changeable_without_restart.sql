-- Tags: no-fasttest
-- no-fasttest: needs Parquet

-- These cache limits are applied on config reload.
SELECT name, changeable_without_restart
FROM system.server_settings
WHERE name IN (
    'parquet_metadata_cache_max_entries',
    'query_cache.max_entries',
    'query_cache.max_entry_size_in_bytes',
    'text_index_header_cache_max_entries',
    'text_index_postings_cache_max_entries',
    'text_index_tokens_cache_max_entries',
    'vector_similarity_index_cache_max_entries')
ORDER BY name;
