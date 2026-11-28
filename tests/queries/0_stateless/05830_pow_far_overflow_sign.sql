-- `pow` must return inf or 0 with the right sign when the result is far out of the `Float64` range,
-- not -inf or a finite garbage value.

SELECT pow(10, 1e19), pow(1.5, 1e19), pow(10, 4e15), pow(10, 1.3e16), pow(10, 1e300);
SELECT pow(10, -1e19), pow(1.5, -1e19), pow(10, -4e15), pow(10, -1.3e16), pow(10, -1e300);
SELECT pow(0.1, 1e19), pow(0.5, -1e19), pow(-10, 1e19), pow(-10, -1e19);

SELECT countIf(isInfinite(pow(10, y)) AND pow(10, y) > 0), countIf(pow(10, -y) = 0)
FROM (SELECT exp10(3 + number / 10) AS y FROM numbers(3000));
