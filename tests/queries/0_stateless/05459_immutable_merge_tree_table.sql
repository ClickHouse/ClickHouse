DROP TABLE IF EXISTS t_immutable;
DROP TABLE IF EXISTS t_immutable_other;
DROP TABLE IF EXISTS t_immutable_ttl;

CREATE TABLE t_immutable (d Date, id UInt64, s String)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY id
SETTINGS immutable = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_immutable_other (d Date, id UInt64, s String)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY id;

INSERT INTO t_immutable VALUES ('2026-01-01', 1, 'a'), ('2026-02-01', 2, 'b');
INSERT INTO t_immutable_other VALUES ('2026-01-01', 10, 'x'), ('2026-03-01', 3, 'c');

-- Mutations.
ALTER TABLE t_immutable UPDATE s = 'z' WHERE 1; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable DELETE WHERE id = 1; -- { serverError IMMUTABLE_TABLE }
DELETE FROM t_immutable WHERE id = 1 SETTINGS lightweight_delete_mode = 'alter_update'; -- { serverError IMMUTABLE_TABLE }
DELETE FROM t_immutable WHERE id = 1 SETTINGS lightweight_delete_mode = 'lightweight_update_force', enable_lightweight_update = 1; -- { serverError IMMUTABLE_TABLE }
UPDATE t_immutable SET s = 'z' WHERE 1 SETTINGS enable_lightweight_update = 1; -- { serverError IMMUTABLE_TABLE }
TRUNCATE TABLE t_immutable; -- { serverError IMMUTABLE_TABLE }

-- Partition commands that remove data.
ALTER TABLE t_immutable DROP PARTITION 202601; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable DETACH PARTITION 202601; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable REPLACE PARTITION 202601 FROM t_immutable_other; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable MOVE PARTITION 202601 TO TABLE t_immutable_other; -- { serverError IMMUTABLE_TABLE }

-- Adding data from another table is allowed.
ALTER TABLE t_immutable ATTACH PARTITION 202603 FROM t_immutable_other;

-- TTL that deletes or resets rows.
ALTER TABLE t_immutable MODIFY TTL d + INTERVAL 1 DAY; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable MODIFY COLUMN s String TTL d + INTERVAL 1 DAY; -- { serverError IMMUTABLE_TABLE }
CREATE TABLE t_immutable_ttl (d Date, id UInt64) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY SETTINGS immutable = 1; -- { serverError IMMUTABLE_TABLE }

-- The setting cannot be changed.
ALTER TABLE t_immutable MODIFY SETTING immutable = 0; -- { serverError READONLY_SETTING }
ALTER TABLE t_immutable RESET SETTING immutable; -- { serverError READONLY_SETTING }

-- Schema additions are allowed, rewriting existing values is not.
ALTER TABLE t_immutable ADD COLUMN v UInt32 DEFAULT 0;
ALTER TABLE t_immutable ADD INDEX idx s TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t_immutable MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
ALTER TABLE t_immutable MODIFY COLUMN v UInt64; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable MATERIALIZE COLUMN v; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable RENAME COLUMN v TO w; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable DROP COLUMN v; -- { serverError IMMUTABLE_TABLE }
ALTER TABLE t_immutable CLEAR COLUMN v IN PARTITION 202601; -- { serverError IMMUTABLE_TABLE }

INSERT INTO t_immutable VALUES ('2026-02-01', 4, 'd', 1);
OPTIMIZE TABLE t_immutable FINAL;

SELECT d, id, s, v FROM t_immutable ORDER BY id;

DROP TABLE t_immutable;
DROP TABLE t_immutable_other;
