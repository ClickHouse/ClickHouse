var current_data_size = 100000000;

var current_systems = ["ClickHouse", "Vertica", "Greenplum"];

var queries =
    [
        {
            "query": "SELECT count() FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT count() FROM hits WHERE AdvEngineID != 0",
            "comment": "",
        },
        {
            "query": "SELECT sum(AdvEngineID), count(), avg(ResolutionWidth) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT sum(UserID) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT uniq(UserID) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT uniq(SearchPhrase) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT min(EventDate), max(EventDate) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT AdvEngineID, count() FROM hits WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY count() DESC",
            "comment": "",
        },
        {
            "query": "SELECT RegionID, uniq(UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT RegionID, sum(AdvEngineID), count() AS c, avg(ResolutionWidth), uniq(UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT MobilePhoneModel, uniq(UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT MobilePhone, MobilePhoneModel, uniq(UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase, uniq(UserID) AS u FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchEngineID, SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT UserID, count() FROM hits GROUP BY UserID ORDER BY count() DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase ORDER BY count() DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT UserID, toMinute(EventTime) AS m, SearchPhrase, count() FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY count() DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT UserID FROM hits WHERE UserID = 12345678901234567890",
            "comment": "",
        },
        {
            "query": "SELECT count() FROM hits WHERE URL LIKE '%metrika%'",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase, any(URL), count() AS c FROM hits WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase, any(URL), any(Title), count() AS c, uniq(UserID) FROM hits WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT * FROM hits WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT CounterID, avg(length(URL)) AS l, count() AS c FROM hits WHERE URL != '' GROUP BY CounterID HAVING c > 100000 ORDER BY l DESC LIMIT 25",
            "comment": "",
        },
        {
            "query": "SELECT domainWithoutWWW(Referer) AS key, avg(length(Referer)) AS l, count() AS c, any(Referer) FROM hits WHERE Referer != '' GROUP BY key HAVING c > 100000 ORDER BY l DESC LIMIT 25",
            "comment": "",
        },
        {
            "query": "SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3), sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7), sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11), sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15), sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19), sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23), sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27), sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31), sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35), sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39), sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43), sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47), sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51), sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55), sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59), sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63), sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67), sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71), sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75), sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79), sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits",
            "comment": "",
        },
        {
            "query": "SELECT SearchEngineID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT URL, count() AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT 1, URL, count() AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT ClientIP AS x, x - 1, x - 2, x - 3, count() AS c FROM hits GROUP BY x, x - 1, x - 2, x - 3 ORDER BY c DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT    URL,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT DontCountHits    AND NOT Refresh    AND notEmpty(URL) GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT    Title,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT DontCountHits    AND NOT Refresh    AND notEmpty(Title) GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT    URL,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND IsLink    AND NOT IsDownload GROUP BY URL ORDER BY PageViews DESC LIMIT 1000",
            "comment": "",
        },
        {
            "query": "SELECT    TraficSourceID,    SearchEngineID,    AdvEngineID,    ((SearchEngineID = 0 AND AdvEngineID = 0) ? Referer : '') AS Src,    URL AS Dst,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh GROUP BY    TraficSourceID,    SearchEngineID,    AdvEngineID,    Src,    Dst ORDER BY PageViews DESC LIMIT 1000",
            "comment": "",
        },
        {
            "query": "SELECT    URLHash,    EventDate,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND TraficSourceID IN (-1, 6)    AND RefererHash = halfMD5('http://yandex.ru/') GROUP BY    URLHash,    EventDate ORDER BY PageViews DESC LIMIT 100",
            "comment": "",
        },
        {
            "query": "SELECT    WindowClientWidth,    WindowClientHeight,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND NOT DontCountHits    AND URLHash = halfMD5('http://yandex.ru/') GROUP BY    WindowClientWidth,    WindowClientHeight ORDER BY PageViews DESC LIMIT 10000;",
            "comment": "",
        },
        {
            "query": "SELECT    toStartOfMinute(EventTime) AS Minute,    count() AS PageViews FROM hits WHERE    CounterID = 34    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-02')    AND NOT Refresh    AND NOT DontCountHits GROUP BY    Minute ORDER BY Minute;",
            "comment": "",
        }
    ]
