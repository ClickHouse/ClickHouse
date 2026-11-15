-- A TimeSeries target in a database named `inner` is formatted to a query that parses back.
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner`.t'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries SAMPLES `INNER`.s'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries METRIC FAMILIES `inner`'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner`.t TAGS INNER ENGINE = Memory'));
