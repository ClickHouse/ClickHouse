CREATE FUNCTION avgWeighted AS (x, w) -> toFloat64(sum(x * w) / sum(w))
