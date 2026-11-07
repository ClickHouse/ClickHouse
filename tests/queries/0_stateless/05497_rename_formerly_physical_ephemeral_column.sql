SET mutations_sync = 2;

DROP TABLE IF EXISTS t_rename_formerly_physical_ephemeral;

CREATE TABLE t_rename_formerly_physical_ephemeral (id UInt64, a String, b String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_rename_formerly_physical_ephemeral VALUES (1, 'old input', 'kept');

-- The old part still stores `a`, but the rename must not read its `EPHEMERAL` destination.
ALTER TABLE t_rename_formerly_physical_ephemeral MODIFY COLUMN a String EPHEMERAL;
ALTER TABLE t_rename_formerly_physical_ephemeral RENAME COLUMN a TO a_old;

-- An ordinary stored column must still be read and preserved by the compact rewrite.
ALTER TABLE t_rename_formerly_physical_ephemeral RENAME COLUMN b TO b_renamed;
SELECT id, b_renamed FROM t_rename_formerly_physical_ephemeral ORDER BY id;

SELECT arraySort(groupUniqArray(column))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_formerly_physical_ephemeral'
    AND active AND column IN ('a', 'a_old', 'b', 'b_renamed');

INSERT INTO t_rename_formerly_physical_ephemeral (id, a_old, b_renamed) VALUES (2, 'new input', 'also kept');
SELECT id, b_renamed FROM t_rename_formerly_physical_ephemeral ORDER BY id;

DROP TABLE t_rename_formerly_physical_ephemeral;
