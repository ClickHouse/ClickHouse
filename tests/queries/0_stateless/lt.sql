SET allow_suspicious_indices = 1;
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id);
ALTER TABLE allow_suspicious_indices ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id, id2 + 1, id2 + 1);
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id);
ALTER TABLE allow_suspicious_indices ADD INDEX idx (id+1, id+1) type minmax;
