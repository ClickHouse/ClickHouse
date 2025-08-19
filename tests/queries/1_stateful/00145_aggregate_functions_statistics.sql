SELECT varSamp(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 0);
SELECT varSamp(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 1);
SELECT round(varSamp(ResolutionWidth), 6) FROM test.hits;

SELECT stddevSamp(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 0);
SELECT stddevSamp(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 1);
SELECT round(stddevSamp(ResolutionWidth), 6) FROM test.hits;

SELECT varPop(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 0);
SELECT varPop(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 1);
SELECT round(varPop(ResolutionWidth), 6) FROM test.hits;

SELECT stddevPop(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 0);
SELECT stddevPop(ResolutionWidth) FROM (SELECT ResolutionWidth FROM test.hits LIMIT 1);
SELECT round(stddevPop(ResolutionWidth), 6) FROM test.hits;

SELECT covarSamp(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 0);
SELECT covarSamp(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 1);
SELECT round(covarSamp(ResolutionWidth, ResolutionHeight), 6) FROM test.hits;

SELECT covarPop(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 0);
SELECT covarPop(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 1);
SELECT round(covarPop(ResolutionWidth, ResolutionHeight), 6) FROM test.hits;

SELECT corr(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 0);
SELECT corr(ResolutionWidth, ResolutionHeight) FROM (SELECT ResolutionWidth, ResolutionHeight FROM test.hits LIMIT 1);
SELECT round(corr(ResolutionWidth, ResolutionHeight), 6) FROM test.hits;

