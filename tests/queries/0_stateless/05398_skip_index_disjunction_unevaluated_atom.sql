-- With `use_skip_indexes_for_disjunctions`, the skip indexes report for each atom of the filter whether it can be
-- true in a granule, and the reports decide which granules the disjunction reads.

SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;

-- An index that cannot evaluate an atom reports it as unknown, so the exclusion that another index found for the
-- atom stays. `x_index` cannot evaluate `abs(x) = 100`, because `abs` is not monotonic over a granule that holds
-- both signs, and `abs_index` excludes the atom from every granule. Only the granule with `y = 1` is read.
CREATE TABLE tab
(
    x Int64,
    y Int64,
    INDEX abs_index abs(x) TYPE minmax,
    INDEX x_index x TYPE minmax,
    INDEX y_index y TYPE minmax
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT number % 21 - 10, number FROM numbers(16384);

SELECT count() FROM tab WHERE abs(x) = 100 OR y = 1 SETTINGS max_rows_to_read = 128;

-- A polygon whose bounding box misses the box of a granule excludes the atom from the granule, and `xy_index`
-- reports that.
CREATE TABLE points
(
    x Float64,
    y Float64,
    z UInt32,
    INDEX xy_index (x, y) TYPE minmax,
    INDEX z_index z TYPE minmax
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0;

INSERT INTO points SELECT number % 100, number % 50, number FROM numbers(16384);

SELECT count() FROM points WHERE pointInPolygon((x, y), [(1000., 1000.), (1001., 1000.), (1001., 1001.)]) OR z = 1
SETTINGS max_rows_to_read = 128;

DROP TABLE tab;
DROP TABLE points;
