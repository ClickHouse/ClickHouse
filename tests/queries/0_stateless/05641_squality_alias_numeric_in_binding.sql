SET allow_lossy_numeric_supertype = 1;

CREATE TABLE squality_alias_numeric_in_binding
(
    d Decimal(18,0), x Float64,
    a1 Decimal(18,0) ALIAS toDecimal64(d IN (materialize(x)),0),
    a2 Decimal(18,0) ALIAS toDecimal64(a1 IN (materialize(x)),0),
    a3 Decimal(18,0) ALIAS toDecimal64(a2 IN (materialize(x)),0),
    a4 Decimal(18,0) ALIAS toDecimal64(a3 IN (materialize(x)),0),
    a5 Decimal(18,0) ALIAS toDecimal64(a4 IN (materialize(x)),0),
    a6 Decimal(18,0) ALIAS toDecimal64(a5 IN (materialize(x)),0),
    a7 Decimal(18,0) ALIAS toDecimal64(a6 IN (materialize(x)),0),
    a8 Decimal(18,0) ALIAS toDecimal64(a7 IN (materialize(x)),0),
    a9 Decimal(18,0) ALIAS toDecimal64(a8 IN (materialize(x)),0),
    a10 Decimal(18,0) ALIAS toDecimal64(a9 IN (materialize(x)),0),
    a11 Decimal(18,0) ALIAS toDecimal64(a10 IN (materialize(x)),0),
    a12 Decimal(18,0) ALIAS toDecimal64(a11 IN (materialize(x)),0),
    a13 Decimal(18,0) ALIAS toDecimal64(a12 IN (materialize(x)),0),
    a14 Decimal(18,0) ALIAS toDecimal64(a13 IN (materialize(x)),0),
    a15 Decimal(18,0) ALIAS toDecimal64(a14 IN (materialize(x)),0),
    a16 Decimal(18,0) ALIAS toDecimal64(a15 IN (materialize(x)),0)
) ENGINE = Memory;

INSERT INTO squality_alias_numeric_in_binding (d,x) VALUES (1,1);
SELECT a16 FROM squality_alias_numeric_in_binding;
DROP TABLE squality_alias_numeric_in_binding;
