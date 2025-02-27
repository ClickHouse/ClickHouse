DROP TABLE IF EXISTS hits_none;
CREATE TABLE hits_none (Title String CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO hits_none SELECT Title FROM test.hits;

SET min_bytes_to_use_mmap_io = 1;
SELECT sum(length(Title)) FROM hits_none;

DROP TABLE hits_none;
