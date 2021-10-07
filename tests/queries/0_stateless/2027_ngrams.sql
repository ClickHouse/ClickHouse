SELECT ngrams('Test', 1);
SELECT ngrams('Test', 2);
SELECT ngrams('Test', 3);
SELECT ngrams('Test', 3);
SELECT ngrams('Test', 5);

SELECT ngrams(materialize('Test'), 1);
SELECT ngrams(materialize('Test'), 2);
SELECT ngrams(materialize('Test'), 3);
SELECT ngrams(materialize('Test'), 3);
SELECT ngrams(materialize('Test'), 5);

SELECT ngrams(toFixedString('Test', 4), 1);
SELECT ngrams(toFixedString('Test', 4), 2);
SELECT ngrams(toFixedString('Test', 4), 3);
SELECT ngrams(toFixedString('Test', 4), 3);
SELECT ngrams(toFixedString('Test', 4), 5);

SELECT ngrams(materialize(toFixedString('Test', 4)), 1);
SELECT ngrams(materialize(toFixedString('Test', 4)), 2);
SELECT ngrams(materialize(toFixedString('Test', 4)), 3);
SELECT ngrams(materialize(toFixedString('Test', 4)), 3);
SELECT ngrams(materialize(toFixedString('Test', 4)), 5);
