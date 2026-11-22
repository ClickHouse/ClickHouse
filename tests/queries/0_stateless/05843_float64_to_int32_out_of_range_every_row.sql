-- The same Float64 outside of the range of Int32 or UInt32 converts to the same value in every row of a block,
-- and in a block of one row and a block of eight rows.
SELECT uniqExact(toInt32(x)), uniqExact(toUInt32(x)), uniqExact(CAST(x AS Int32)), uniqExact(CAST(x AS UInt32))
FROM (SELECT materialize(1e11) AS x FROM numbers(5));

SELECT uniqExact(toInt32(x)), uniqExact(CAST(x AS Int32))
FROM (SELECT materialize(-81183017266.28046) AS x FROM numbers(5));

SELECT
    (SELECT toInt32(materialize(1e11))) = (SELECT any(toInt32(x)) FROM (SELECT materialize(1e11) AS x FROM numbers(8))),
    (SELECT toUInt32(materialize(1e11))) = (SELECT any(toUInt32(x)) FROM (SELECT materialize(1e11) AS x FROM numbers(8)));
