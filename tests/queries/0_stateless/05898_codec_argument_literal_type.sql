-- A literal of the wrong type in a codec argument must be rejected as a codec parameter error, not BAD_GET.

CREATE TABLE t_t64_uint (v Int64 CODEC(T64(1))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_t64_float (v Int64 CODEC(T64(1.5))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_t64_null (v Int64 CODEC(T64(NULL))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_t64_bool (v Int64 CODEC(T64(true))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_t64_merge_tree (v Int64 CODEC(T64(9))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

CREATE TABLE t_zstd_string (v Int64 CODEC(ZSTD('a'))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_zstd_float (v Int64 CODEC(ZSTD(1.5))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_zstd_window_string (v Int64 CODEC(ZSTD(1, 'a'))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_lz4hc_string (v Int64 CODEC(LZ4HC('a'))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_lz4hc_float (v Int64 CODEC(LZ4HC(1.5))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_zxc_string (v Int64 CODEC(ZXC('a'))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

DROP TABLE IF EXISTS t_codec_valid;
CREATE TABLE t_codec_valid
(
    a Int64 CODEC(T64('bit')),
    b Int64 CODEC(ZSTD(-1)),
    c Int64 CODEC(ZSTD(true)),
    d Int64 CODEC(ZSTD(3, 20)),
    e Int64 CODEC(LZ4HC(-1)),
    f Int64 CODEC(LZ4HC(true))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_valid VALUES (1, 2, 3, 4, 5, 6);
DETACH TABLE t_codec_valid;
ATTACH TABLE t_codec_valid;
SELECT * FROM t_codec_valid;
DROP TABLE t_codec_valid;
