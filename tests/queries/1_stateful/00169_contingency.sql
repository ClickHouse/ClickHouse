WITH URLDomain AS a, URLDomain AS b
SELECT round(cramersV(a, b), 2), round(cramersVBiasCorrected(a, b), 2), round(theilsU(a, b), 2), round(theilsU(b, a), 2), round(contingency(a, b), 2) FROM test.hits;

WITH URLDomain AS a, RefererDomain AS b
SELECT round(cramersV(a, b), 2), round(cramersVBiasCorrected(a, b), 2), round(theilsU(a, b), 2), round(theilsU(b, a), 2), round(contingency(a, b), 2) FROM test.hits;

WITH URLDomain AS a, CounterID AS b
SELECT round(cramersV(a, b), 2), round(cramersVBiasCorrected(a, b), 2), round(theilsU(a, b), 2), round(theilsU(b, a), 2), round(contingency(a, b), 2) FROM test.hits;

WITH ClientIP AS a, RemoteIP AS b
SELECT round(cramersV(a, b), 2), round(cramersVBiasCorrected(a, b), 2), round(theilsU(a, b), 2), round(theilsU(b, a), 2), round(contingency(a, b), 2) FROM test.hits;

WITH ResolutionWidth AS a, ResolutionHeight AS b
SELECT round(cramersV(a, b), 2), round(cramersVBiasCorrected(a, b), 2), round(theilsU(a, b), 2), round(theilsU(b, a), 2), round(contingency(a, b), 2) FROM test.hits;
