-- Parameter and argument validation of the aggregate function MVTEncode, which assembles a binary
-- Mapbox Vector Tile layer. The accepted shape is
--   MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])
-- as documented in the FunctionDocumentation block of registerAggregateFunctionMVTEncode.
-- The parameters are checked when the aggregate function is created, before any argument type is looked
-- at, so the error cases below pass a well-formed geometry and still fail on the parameter alone.
-- Only error code names and tile equalities are asserted, never the tile bytes or their length.

SELECT '-- the argument list is a geometry and an optional properties tuple, so a third argument is rejected';
SELECT MVTEncode('l')((0., 0.)::Point::Geometry, tuple(1), tuple(2)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- at most four parameters are accepted';
SELECT MVTEncode('l', 4096, 'f', 1, 1)((0., 0.)::Point::Geometry); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

SELECT '-- the first parameter (layer_name) must be a string';
SELECT MVTEncode(1)((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- the second parameter (extent) must be an integer, and unsigned values outside [1, 2147483647] are rejected';
SELECT MVTEncode('l', 0)((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }
SELECT MVTEncode('l', 2147483648)((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }
SELECT MVTEncode('l', 'x')((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- a signed extent is held to the same range';
-- A negative literal reaches the factory as a signed field; toInt64 forces the signed field for a
-- value that would otherwise be unsigned.
SELECT MVTEncode('l', -1)((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }
SELECT MVTEncode('l', toInt64(2147483648))((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- the upper bound of the extent range is inclusive and both spellings encode the same tile';
SELECT MVTEncode('l', 2147483647)((0., 0.)::Point::Geometry)
     = MVTEncode('l', toInt64(2147483647))((0., 0.)::Point::Geometry);

SELECT '-- the third parameter (feature_id_name) must be a string';
SELECT MVTEncode('l', 4096, 1)((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- the fourth parameter (stringify_unsupported) must be an integer';
SELECT MVTEncode('l', 4096, 'f', 'x')((0., 0.)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- any non-zero stringify_unsupported enables stringification, signed or unsigned alike';
-- An Int128 property is encodable only with stringification on, so both sides of the equality need it.
SELECT MVTEncode('l', 4096, '', -1)((0., 0.)::Point::Geometry, tuple(toInt128(7))::Tuple(big Int128))
     = MVTEncode('l', 4096, '', 1)((0., 0.)::Point::Geometry, tuple(toInt128(7))::Tuple(big Int128));

SELECT '-- a signed zero leaves stringification off, so the Int128 property stays unsupported';
SELECT MVTEncode('l', 4096, '', toInt64(0))((0., 0.)::Point::Geometry, tuple(toInt128(7))::Tuple(big Int128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
