-- `random` of PostgreSQL returns a `double precision` value in the range [0, 1). It is an alias of `randCanonical`.
SELECT toTypeName(random()), toTypeName(RANDOM());
SELECT min(r) >= 0, max(r) < 1, uniqExact(r) > 1 FROM (SELECT random() AS r FROM numbers(1000));
