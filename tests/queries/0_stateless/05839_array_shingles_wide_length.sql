SELECT arrayShingles([1, 2, 3], toUInt128('18446744073709551618')); -- { serverError BAD_ARGUMENTS }
SELECT arrayShingles(materialize([1, 2, 3]), materialize(toInt256('-18446744073709551614'))); -- { serverError BAD_ARGUMENTS }
SELECT arrayShingles([1, 2, 3], toUInt128(2)), arrayShingles([1, 2, 3], toInt256(3));
