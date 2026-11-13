SET in_null_as_unknown = 1;

SELECT toFloat64('nan') IN (toFloat64('nan'), NULL),
       toFloat64('nan') IN (materialize(toFloat64('nan')), NULL);
SELECT toFloat64('nan') NOT IN (toFloat64('nan'), NULL),
       toFloat64('nan') NOT IN (materialize(toFloat64('nan')), NULL);
SELECT toFloat64('nan') IN (toFloat64('nan')),
       toFloat64('nan') IN (materialize(toFloat64('nan')));
SELECT count() FROM values('x Float64', ('nan'))
WHERE x IN (toFloat64('nan'), NULL);
SELECT count() FROM values('x Float64', ('nan'))
WHERE x IN (materialize(toFloat64('nan')), NULL);

SELECT toBFloat16('nan') IN (toBFloat16('nan'), NULL),
       toBFloat16('nan') IN (materialize(toBFloat16('nan')), NULL);
