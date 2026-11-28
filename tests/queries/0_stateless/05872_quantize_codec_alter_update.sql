-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- `ALTER TABLE ... UPDATE` of a column with a `Quantized` codec must write the new part with the codec's streams
-- (`<column>.quantized`, `<column>.product_quantization_codebook`). The mutation header holds a type without the
-- custom serialization of the codec, and using it for the new part dropped those streams, so reading them failed.

SET enable_quantized_codec = 1;
SET mutations_sync = 1;
SET vector_search_use_quantized_codes = 1;

DROP TABLE IF EXISTS t_quantized_update_pq;
CREATE TABLE t_quantized_update_pq (id UInt64, v Array(Float32) CODEC(Quantized('product', 8, 2, 2)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_quantized_update_pq SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);
ALTER TABLE t_quantized_update_pq UPDATE v = arrayMap(x -> -x, v) WHERE id % 3 = 0;

SELECT 'pq', has(substreams, 'v.quantized'), has(substreams, 'v.product_quantization_codebook')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_quantized_update_pq' AND active AND column = 'v';
SELECT 'pq', count(), sum(length(v.quantized)), countIf(length(v.product_quantization_codebook) = 128) FROM t_quantized_update_pq;
SELECT 'pq', count() FROM (SELECT id FROM t_quantized_update_pq ORDER BY L2Distance(v, [1., 1., 1., 1., 1., 1., 1., 1.]::Array(Float32)) LIMIT 5);

DROP TABLE t_quantized_update_pq;

DROP TABLE IF EXISTS t_quantized_update_rabitq;
DROP TABLE IF EXISTS t_quantized_update_rabitq_reference;
CREATE TABLE t_quantized_update_rabitq (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 8)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
CREATE TABLE t_quantized_update_rabitq_reference (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 8)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_quantized_update_rabitq SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);
ALTER TABLE t_quantized_update_rabitq UPDATE v = arrayMap(x -> -x, v) WHERE id % 3 = 0;
INSERT INTO t_quantized_update_rabitq_reference SELECT id, v FROM t_quantized_update_rabitq;

SELECT 'rabitq', part_type, has(substreams, 'v.quantized')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_quantized_update_rabitq' AND active AND column = 'v';
-- The codes of the mutated part match the codes of a part freshly written from the same vectors.
SELECT 'rabitq',
    (SELECT groupArray(q) FROM (SELECT v.quantized AS q FROM t_quantized_update_rabitq ORDER BY id))
    = (SELECT groupArray(q) FROM (SELECT v.quantized AS q FROM t_quantized_update_rabitq_reference ORDER BY id));

DROP TABLE t_quantized_update_rabitq;
DROP TABLE t_quantized_update_rabitq_reference;
