SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40

SYSTEM FLUSH LOGS;

-- We had a bug which lead to additional compressed data read. hits compressed size if about 1.2G, but we read more then 3GB.
-- Small additional reads still possible, so we compare with about 1.5Gb.
SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 1500000000 from system.query_log where query = 'SELECT sum(cityHash64(*)) FROM datasets.hits_v1 SETTINGS max_threads=40' and type = 'QueryFinish'
