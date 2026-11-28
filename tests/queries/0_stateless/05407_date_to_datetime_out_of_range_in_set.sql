-- A `Date` / `Date32` constant whose midnight is outside the range of `DateTime` cannot equal any `DateTime`
-- value. It used to be truncated modulo 2^32 when the `IN` set was built, so it matched an unrelated row:
-- `0000-01-01` became `2257290240` (2041-07-12 UTC), and the `Date` maximum `2149-06-06` became
-- `1367256704` (2013-04-29 UTC).

DROP TABLE IF EXISTS t_date_to_datetime_in_set;
CREATE TABLE t_date_to_datetime_in_set (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_date_to_datetime_in_set VALUES (2257290240), (1367256704), (0);

-- Before the epoch: no match, consistent with `=`.
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toDate32('0000-01-01'));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt NOT IN (toDate32('0000-01-01'));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt = toDate32('0000-01-01');
SELECT count() FROM t_date_to_datetime_in_set WHERE materialize(dt) IN (toDate32('0000-01-01'));

-- After the last `DateTime` second: no match, consistent with `=`.
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toDate('2149-06-06'));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt NOT IN (toDate('2149-06-06'));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt = toDate('2149-06-06');

-- Wrapping either side must not restore the match.
DROP TABLE IF EXISTS t_date_to_datetime_in_set_wrapped;
CREATE TABLE t_date_to_datetime_in_set_wrapped (dt Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_date_to_datetime_in_set_wrapped VALUES (2257290240), (1367256704), (0);
SELECT count() FROM t_date_to_datetime_in_set_wrapped WHERE dt IN (toDate32('0000-01-01'));
SELECT count() FROM t_date_to_datetime_in_set_wrapped WHERE dt IN (toDate('2149-06-06'));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toNullable(toDate32('0000-01-01')));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toLowCardinality(toDate('2149-06-06')));
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toDate32('0000-01-01')::Dynamic);
DROP TABLE t_date_to_datetime_in_set_wrapped;

-- In-range values still match.
SELECT count() FROM t_date_to_datetime_in_set WHERE dt IN (toDate32('1970-01-01'), toDate32('0000-01-01'), toDate('2149-06-06'));

DROP TABLE t_date_to_datetime_in_set;
