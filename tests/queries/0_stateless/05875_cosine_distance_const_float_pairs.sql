-- cosineDistance with a constant argument must match the non-constant path for every pair of floating-point
-- element types, in both argument orders, for vectors with only a tail (7), one full chunk (8), chunks and a
-- tail (17) and a typical embedding size (768).

DROP TABLE IF EXISTS cosine_const;
CREATE TABLE cosine_const (dim UInt32, bf16 Array(BFloat16), f32 Array(Float32), f64 Array(Float64)) ENGINE = Memory;
INSERT INTO cosine_const
SELECT dim, CAST(v AS Array(BFloat16)), CAST(v AS Array(Float32)), v
FROM (SELECT arrayJoin([7, 8, 17, 768]) AS dim, arrayMap(i -> sin(number * 0.7 + i * 1.3) * (1 + number % 5), range(dim)) AS v FROM numbers(64));

WITH
    arrayMap(i -> cos(i * 0.37), range(7)) AS c64,
    CAST(c64 AS Array(Float32)) AS c32,
    CAST(c64 AS Array(BFloat16)) AS cbf
SELECT
    7,
    isConstant(c64) AND isConstant(c32) AND isConstant(cbf),
    max(abs(cosineDistance(bf16, c64) - cosineDistance(bf16, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, bf16) - cosineDistance(materialize(c64), bf16))) < 1e-12,
    max(abs(cosineDistance(bf16, c32) - cosineDistance(bf16, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, bf16) - cosineDistance(materialize(c32), bf16))) < 1e-5,
    max(abs(cosineDistance(bf16, cbf) - cosineDistance(bf16, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, bf16) - cosineDistance(materialize(cbf), bf16))) < 1e-5,
    max(abs(cosineDistance(f32, c64) - cosineDistance(f32, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f32) - cosineDistance(materialize(c64), f32))) < 1e-12,
    max(abs(cosineDistance(f32, c32) - cosineDistance(f32, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, f32) - cosineDistance(materialize(c32), f32))) < 1e-5,
    max(abs(cosineDistance(f32, cbf) - cosineDistance(f32, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, f32) - cosineDistance(materialize(cbf), f32))) < 1e-5,
    max(abs(cosineDistance(f64, c64) - cosineDistance(f64, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f64) - cosineDistance(materialize(c64), f64))) < 1e-12,
    max(abs(cosineDistance(f64, c32) - cosineDistance(f64, materialize(c32)))) < 1e-12,
    max(abs(cosineDistance(c32, f64) - cosineDistance(materialize(c32), f64))) < 1e-12,
    max(abs(cosineDistance(f64, cbf) - cosineDistance(f64, materialize(cbf)))) < 1e-12,
    max(abs(cosineDistance(cbf, f64) - cosineDistance(materialize(cbf), f64))) < 1e-12
FROM cosine_const WHERE dim = 7;

WITH
    arrayMap(i -> cos(i * 0.37), range(8)) AS c64,
    CAST(c64 AS Array(Float32)) AS c32,
    CAST(c64 AS Array(BFloat16)) AS cbf
SELECT
    8,
    isConstant(c64) AND isConstant(c32) AND isConstant(cbf),
    max(abs(cosineDistance(bf16, c64) - cosineDistance(bf16, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, bf16) - cosineDistance(materialize(c64), bf16))) < 1e-12,
    max(abs(cosineDistance(bf16, c32) - cosineDistance(bf16, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, bf16) - cosineDistance(materialize(c32), bf16))) < 1e-5,
    max(abs(cosineDistance(bf16, cbf) - cosineDistance(bf16, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, bf16) - cosineDistance(materialize(cbf), bf16))) < 1e-5,
    max(abs(cosineDistance(f32, c64) - cosineDistance(f32, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f32) - cosineDistance(materialize(c64), f32))) < 1e-12,
    max(abs(cosineDistance(f32, c32) - cosineDistance(f32, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, f32) - cosineDistance(materialize(c32), f32))) < 1e-5,
    max(abs(cosineDistance(f32, cbf) - cosineDistance(f32, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, f32) - cosineDistance(materialize(cbf), f32))) < 1e-5,
    max(abs(cosineDistance(f64, c64) - cosineDistance(f64, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f64) - cosineDistance(materialize(c64), f64))) < 1e-12,
    max(abs(cosineDistance(f64, c32) - cosineDistance(f64, materialize(c32)))) < 1e-12,
    max(abs(cosineDistance(c32, f64) - cosineDistance(materialize(c32), f64))) < 1e-12,
    max(abs(cosineDistance(f64, cbf) - cosineDistance(f64, materialize(cbf)))) < 1e-12,
    max(abs(cosineDistance(cbf, f64) - cosineDistance(materialize(cbf), f64))) < 1e-12
FROM cosine_const WHERE dim = 8;

WITH
    arrayMap(i -> cos(i * 0.37), range(17)) AS c64,
    CAST(c64 AS Array(Float32)) AS c32,
    CAST(c64 AS Array(BFloat16)) AS cbf
SELECT
    17,
    isConstant(c64) AND isConstant(c32) AND isConstant(cbf),
    max(abs(cosineDistance(bf16, c64) - cosineDistance(bf16, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, bf16) - cosineDistance(materialize(c64), bf16))) < 1e-12,
    max(abs(cosineDistance(bf16, c32) - cosineDistance(bf16, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, bf16) - cosineDistance(materialize(c32), bf16))) < 1e-5,
    max(abs(cosineDistance(bf16, cbf) - cosineDistance(bf16, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, bf16) - cosineDistance(materialize(cbf), bf16))) < 1e-5,
    max(abs(cosineDistance(f32, c64) - cosineDistance(f32, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f32) - cosineDistance(materialize(c64), f32))) < 1e-12,
    max(abs(cosineDistance(f32, c32) - cosineDistance(f32, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, f32) - cosineDistance(materialize(c32), f32))) < 1e-5,
    max(abs(cosineDistance(f32, cbf) - cosineDistance(f32, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, f32) - cosineDistance(materialize(cbf), f32))) < 1e-5,
    max(abs(cosineDistance(f64, c64) - cosineDistance(f64, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f64) - cosineDistance(materialize(c64), f64))) < 1e-12,
    max(abs(cosineDistance(f64, c32) - cosineDistance(f64, materialize(c32)))) < 1e-12,
    max(abs(cosineDistance(c32, f64) - cosineDistance(materialize(c32), f64))) < 1e-12,
    max(abs(cosineDistance(f64, cbf) - cosineDistance(f64, materialize(cbf)))) < 1e-12,
    max(abs(cosineDistance(cbf, f64) - cosineDistance(materialize(cbf), f64))) < 1e-12
FROM cosine_const WHERE dim = 17;

WITH
    arrayMap(i -> cos(i * 0.37), range(768)) AS c64,
    CAST(c64 AS Array(Float32)) AS c32,
    CAST(c64 AS Array(BFloat16)) AS cbf
SELECT
    768,
    isConstant(c64) AND isConstant(c32) AND isConstant(cbf),
    max(abs(cosineDistance(bf16, c64) - cosineDistance(bf16, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, bf16) - cosineDistance(materialize(c64), bf16))) < 1e-12,
    max(abs(cosineDistance(bf16, c32) - cosineDistance(bf16, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, bf16) - cosineDistance(materialize(c32), bf16))) < 1e-5,
    max(abs(cosineDistance(bf16, cbf) - cosineDistance(bf16, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, bf16) - cosineDistance(materialize(cbf), bf16))) < 1e-5,
    max(abs(cosineDistance(f32, c64) - cosineDistance(f32, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f32) - cosineDistance(materialize(c64), f32))) < 1e-12,
    max(abs(cosineDistance(f32, c32) - cosineDistance(f32, materialize(c32)))) < 1e-5,
    max(abs(cosineDistance(c32, f32) - cosineDistance(materialize(c32), f32))) < 1e-5,
    max(abs(cosineDistance(f32, cbf) - cosineDistance(f32, materialize(cbf)))) < 1e-5,
    max(abs(cosineDistance(cbf, f32) - cosineDistance(materialize(cbf), f32))) < 1e-5,
    max(abs(cosineDistance(f64, c64) - cosineDistance(f64, materialize(c64)))) < 1e-12,
    max(abs(cosineDistance(c64, f64) - cosineDistance(materialize(c64), f64))) < 1e-12,
    max(abs(cosineDistance(f64, c32) - cosineDistance(f64, materialize(c32)))) < 1e-12,
    max(abs(cosineDistance(c32, f64) - cosineDistance(materialize(c32), f64))) < 1e-12,
    max(abs(cosineDistance(f64, cbf) - cosineDistance(f64, materialize(cbf)))) < 1e-12,
    max(abs(cosineDistance(cbf, f64) - cosineDistance(materialize(cbf), f64))) < 1e-12
FROM cosine_const WHERE dim = 768;

-- Zero norm, NaN and empty arrays give NaN; identical, orthogonal and opposite vectors give exact results.
SELECT
    isNaN(cosineDistance(materialize(CAST(range(1, 10) AS Array(BFloat16))), arrayWithConstant(9, 0.))),
    isNaN(cosineDistance(arrayWithConstant(17, 0.), materialize(CAST(range(1, 18) AS Array(Float32))))),
    isNaN(cosineDistance(materialize(arrayWithConstant(9, toBFloat16(0))), CAST(range(1, 10) AS Array(Float64)))),
    isNaN(cosineDistance(CAST(range(1, 18) AS Array(BFloat16)), materialize(arrayWithConstant(17, toFloat32(0))))),
    isNaN(cosineDistance(materialize(CAST(range(1, 18) AS Array(Float64))), CAST(arrayConcat([nan], range(2, 18)) AS Array(BFloat16)))),
    isNaN(cosineDistance(CAST(arrayConcat(range(1, 9), [nan]) AS Array(Float32)), materialize(CAST(range(1, 10) AS Array(BFloat16))))),
    isNaN(cosineDistance(CAST([] AS Array(Float64)), materialize(CAST([] AS Array(BFloat16))))),
    isNaN(cosineDistance(materialize(CAST([] AS Array(Float32))), CAST([] AS Array(BFloat16)))),
    cosineDistance(materialize(CAST(range(1, 18) AS Array(BFloat16))), CAST(range(1, 18) AS Array(Float64))) = 0,
    cosineDistance(CAST(range(1, 10) AS Array(Float32)), materialize(CAST(range(1, 10) AS Array(BFloat16)))) = 0,
    cosineDistance(materialize(CAST(arrayMap(i -> if(i = 0, 1., 0.), range(9)) AS Array(BFloat16))), CAST(arrayMap(i -> if(i = 1, 1., 0.), range(9)) AS Array(Float64))) = 1,
    cosineDistance(CAST(arrayMap(i -> if(i = 16, 1., 0.), range(17)) AS Array(BFloat16)), materialize(CAST(arrayMap(i -> if(i = 0, 1., 0.), range(17)) AS Array(Float32)))) = 1,
    cosineDistance(materialize(CAST(range(1, 18) AS Array(Float32))), CAST(arrayMap(i -> -i, range(1, 18)) AS Array(Float64))) = 2,
    cosineDistance(CAST(arrayMap(i -> -i, range(1, 10)) AS Array(BFloat16)), materialize(CAST(range(1, 10) AS Array(Float32)))) = 2;

DROP TABLE cosine_const;
