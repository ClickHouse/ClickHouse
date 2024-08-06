SELECT
    UserAgent,
    UserID % 3,
    CounterID % 4,
    count() AS TotalEvents,
    avg(ResolutionWidth) AS AvgResolutionWidth,
    count(BY UserAgent) AS TotalEventsByUserAgent,
    sum(JavaEnable BY UserAgent, CounterID % 4) AS SumJavaEnableByUserAgentCounterID,
    min(ClientIP) AS MinClientIP,
    uniqExact(Referer BY UserAgent, CounterID % 4) AS UniqueReferersByUserAgentCounterID,
    count(TOTALS) AS TotalRecords
FROM
    test.hits
WHERE UserAgent < 10
GROUP BY
    UserID % 3,
    CounterID % 4,
    UserAgent
ORDER BY
    UserAgent,
    UserID % 3,
    CounterID % 4