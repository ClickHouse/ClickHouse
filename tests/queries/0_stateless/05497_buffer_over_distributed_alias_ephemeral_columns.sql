-- Tags: distributed

-- A `Buffer` over a `Distributed` table plans its buffers with a copy of the query, which must read only the columns the buffers provide.

-- An ALIAS column smaller than the physical ones.
CREATE TABLE dst_a (s String, a UInt8 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist_a AS dst_a ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst_a);
CREATE TABLE buf_a AS dst_a ENGINE = Buffer(currentDatabase(), dist_a, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO dst_a VALUES ('x');
INSERT INTO buf_a VALUES ('y');
SELECT count() FROM buf_a SETTINGS optimize_trivial_count_query = 0;
SELECT a, s FROM buf_a ORDER BY s;

-- An EPHEMERAL column smaller than the physical ones.
CREATE TABLE dst_e (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist_e AS dst_e ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst_e);
CREATE TABLE buf_e (s String, e UInt8 EPHEMERAL) ENGINE = Buffer(currentDatabase(), dist_e, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO dst_e VALUES ('x');
INSERT INTO buf_e (s) VALUES ('y');
SELECT count() FROM buf_e SETTINGS optimize_trivial_count_query = 0;

-- Physical columns larger than the `_table` virtual column.
CREATE TABLE dst_v (s FixedString(200)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist_v AS dst_v ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst_v);
CREATE TABLE buf_v AS dst_v ENGINE = Buffer(currentDatabase(), dist_v, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO dst_v VALUES ('x');
INSERT INTO buf_v VALUES ('y');
SELECT count() FROM buf_v SETTINGS optimize_trivial_count_query = 0;
SELECT _table FROM buf_v WHERE s = 'y';
