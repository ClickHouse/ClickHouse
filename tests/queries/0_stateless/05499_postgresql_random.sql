SELECT toTypeName(random());
SELECT toTypeName(RANDOM());
SELECT toTypeName(RaNdOm());
SELECT min(random()) >= 0 AND max(random()) < 1 FROM numbers(1024);
SELECT uniqExact(random()) > 1 FROM numbers(1024);
SELECT count(random()) = count() FROM numbers(1024);
SELECT random(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT RANDOM(NULL); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT random(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
