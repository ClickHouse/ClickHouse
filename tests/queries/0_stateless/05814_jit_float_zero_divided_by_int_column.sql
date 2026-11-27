-- JIT-compiled `0 / x` must give NaN for `x = 0`. LLVM 23 folded `+0.0 / sitofp(x)` to `copysign(0.0, x)`,
-- because `sitofp` is never NaN, ignoring that it can be zero: https://github.com/llvm/llvm-project/pull/202482

SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT c0, c0 * (0 / c0), toFloat64(c0) * (0 / toFloat64(c0)), c0 * ((c0 - c0) / (-c0)), 0 / c0
FROM values('c0 Int32', 0, 2, -3);

SELECT c0, c0 * (0 / c0), 0 / c0
FROM values('c0 Int32', 0, 2, -3)
SETTINGS compile_expressions = 0;
