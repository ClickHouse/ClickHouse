-- A query parameter is not accepted in the name of a TimeSeries target table: the name has no place to keep it.
SELECT formatQuerySingleLine('CREATE TABLE {t:Identifier} ENGINE = TimeSeries TAGS db.tags');
SELECT formatQuerySingleLine('CREATE TABLE t ENGINE = TimeSeries TAGS {t:Identifier}'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TABLE t ENGINE = TimeSeries SAMPLES {d:Identifier}.s'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TABLE t ENGINE = TimeSeries METRICS db.{m:Identifier}'); -- { serverError SYNTAX_ERROR }
CREATE TABLE t ENGINE = TimeSeries TAGS {t:Identifier} UUID '11111111-1111-1111-1111-111111111111'; -- { clientError SYNTAX_ERROR }
CREATE TABLE t ENGINE = TimeSeries RECENT SAMPLES {t:Identifier}; -- { clientError SYNTAX_ERROR }
