var current_data_size = 0;

var current_systems = [];

var queries =
    [
        {
            "query": "SELECT machine_name, MIN(cpu) AS cpu_min, MAX(cpu) AS cpu_max, AVG(cpu) AS cpu_avg, MIN(net_in) AS net_in_min, MAX(net_in) AS net_in_max, AVG(net_in) AS net_in_avg, MIN(net_out) AS net_out_min, MAX(net_out) AS net_out_max, AVG(net_out) AS net_out_avg FROM ( SELECT machine_name, ifNull(cpu_user, 0.0) AS cpu, ifNull(bytes_in, 0.0) AS net_in, ifNull(bytes_out, 0.0) AS net_out FROM mgbench.logs1 WHERE machine_name IN ('anansi','aragog','urd') AND log_time >= toDateTime('2017-01-11 00:00:00')) AS r GROUP BY machine_name;",
            "comment": "Q1.1: What is the CPU/network utilization for each web server since midnight?",
        },
        {
            "query": "SELECT machine_name, log_time FROM mgbench.logs1 WHERE (machine_name LIKE 'cslab%' OR machine_name LIKE 'mslab%') AND load_one IS NULL AND log_time >= toDateTime('2017-01-10 00:00:00') ORDER BY machine_name, log_time;",
            "comment": "Q1.2: Which computer lab machines have been offline in the past day?",
        },
        {
            "query": "SELECT dt, hr, AVG(load_fifteen) AS load_fifteen_avg, AVG(load_five) AS load_five_avg, AVG(load_one) AS load_one_avg, AVG(mem_free) AS mem_free_avg, AVG(swap_free) AS swap_free_avg FROM ( SELECT CAST(log_time AS DATE) AS dt, toHour(log_time) AS hr, load_fifteen, load_five, load_one, mem_free, swap_free FROM mgbench.logs1 WHERE machine_name = 'babbage' AND load_fifteen IS NOT NULL AND load_five IS NOT NULL AND load_one IS NOT NULL AND mem_free IS NOT NULL AND swap_free IS NOT NULL AND log_time >= toDateTime('2017-01-01 00:00:00')) AS r GROUP BY dt, hr ORDER BY dt, hr;",
            "comment": "Q1.3: What are the hourly average metrics during the past 10 days for a specific workstation?",
        },
        {
            "query": "SELECT machine_name, COUNT(*) AS spikes FROM mgbench.logs1 WHERE machine_group = 'Servers' AND cpu_wio > 0.99 AND log_time >= toDateTime('2016-12-01 00:00:00') AND log_time < toDateTime('2017-01-01 00:00:00') GROUP BY machine_name ORDER BY spikes DESC LIMIT 10;",
            "comment": "Q1.4: Over 1 month, how often was each server blocked on disk I/O?",
        },
        {
            "query": "SELECT machine_name, dt, MIN(mem_free) AS mem_free_min FROM ( SELECT machine_name, CAST(log_time AS DATE) AS dt, mem_free FROM mgbench.logs1 WHERE machine_group = 'DMZ' AND mem_free IS NOT NULL ) AS r GROUP BY machine_name, dt HAVING MIN(mem_free) < 10000 ORDER BY machine_name, dt;",
            "comment": "Q1.5: Which externally reachable VMs have run low on memory?",
        },
        {
            "query": "SELECT dt, hr, SUM(net_in) AS net_in_sum, SUM(net_out) AS net_out_sum, SUM(net_in) + SUM(net_out) AS both_sum FROM ( SELECT CAST(log_time AS DATE) AS dt, toHour(log_time) AS hr, ifNull(bytes_in, 0.0) / 1000000000.0 AS net_in, ifNull(bytes_out, 0.0) / 1000000000.0 AS net_out FROM mgbench.logs1 WHERE machine_name IN ('allsorts','andes','bigred','blackjack','bonbon','cadbury','chiclets','cotton','crows','dove','fireball','hearts','huey','lindt','milkduds','milkyway','mnm','necco','nerds','orbit','peeps','poprocks','razzles','runts','smarties','smuggler','spree','stride','tootsie','trident','wrigley','york') ) AS r GROUP BY dt, hr ORDER BY both_sum DESC LIMIT 10;",
            "comment": "Q1.6: What is the total hourly network traffic across all file servers?",
        },
        {
            "query": "SELECT * FROM mgbench.logs2 WHERE status_code >= 500 AND log_time >= toDateTime('2012-12-18 00:00:00') ORDER BY log_time;",
            "comment": "Q2.1: Which requests have caused server errors within the past 2 weeks?",
        },
        {
            "query": "SELECT * FROM mgbench.logs2 WHERE status_code >= 200 AND status_code < 300 AND request LIKE '%/etc/passwd%' AND log_time >= toDateTime('2012-05-06 00:00:00') AND log_time < toDateTime('2012-05-20 00:00:00');",
            "comment": "Q2.3: What was the average path depth for top-level requests in the past month?",
        },
        {
            "query": "SELECT top_level, AVG(length(request) - length(replaceOne(request, '/',''))) AS depth_avg FROM ( SELECT substring(request, 1, len) AS top_level, request FROM ( SELECT position('/', substring(request, 2)) AS len, request FROM mgbench.logs2 WHERE status_code >= 200 AND status_code < 300 AND log_time >= toDateTime('2012-12-01 00:00:00')) AS r WHERE len > 0 ) AS s WHERE top_level IN ('/about','/courses','/degrees','/events','/grad','/industry','/news','/people','/publications','/research','/teaching','/ugrad') GROUP BY top_level ORDER BY top_level;",
            "comment": "Q2.2: During a specific 2-week period, was the user password file leaked?",
        },
        {
            "query": "SELECT client_ip, COUNT(*) AS num_requests FROM mgbench.logs2 WHERE log_time >= toDateTime('2012-10-01 00:00:00') GROUP BY client_ip HAVING COUNT(*) >= 100000 ORDER BY num_requests DESC;",
            "comment": "Q2.4: During the last 3 months, which clients have made an excessive number of requests?",
        },
        {
            "query": "SELECT dt, COUNT(DISTINCT client_ip) FROM ( SELECT CAST(log_time AS DATE) AS dt, client_ip FROM mgbench.logs2) AS r GROUP BY dt ORDER BY dt;",
            "comment": "Q2.5: What are the daily unique visitors?",
        },
        {
            "query": "SELECT AVG(transfer) / 125000000.0 AS transfer_avg, MAX(transfer) / 125000000.0 AS transfer_max FROM ( SELECT log_time, SUM(object_size) AS transfer FROM mgbench.logs2 GROUP BY log_time) AS r;",
            "comment": "Q2.6: What are the average and maximum data transfer rates (Gbps)?",
        },
        {
            "query": "SELECT * FROM mgbench.logs3 WHERE event_type = 'temperature' AND event_value <= 32.0 AND log_time >= '2019-11-29 17:00:00';",
            "comment": "Q3.1: Did the indoor temperature reach freezing over the weekend?",
        },
        {
            "query": "SELECT device_name, device_floor, COUNT(*) AS ct FROM mgbench.logs3 WHERE event_type = 'door_open' AND log_time >= '2019-06-01 00:00:00' GROUP BY device_name, device_floor ORDER BY ct DESC;",
            "comment": "Q3.4: Over the past 6 months, how frequently were each door opened?",
        },
        {
            "query": "SELECT yr, mo, SUM(coffee_hourly_avg) AS coffee_monthly_sum, AVG(coffee_hourly_avg) AS coffee_monthly_avg, SUM(printer_hourly_avg) AS printer_monthly_sum, AVG(printer_hourly_avg) AS printer_monthly_avg, SUM(projector_hourly_avg) AS projector_monthly_sum, AVG(projector_hourly_avg) AS projector_monthly_avg, SUM(vending_hourly_avg) AS vending_monthly_sum, AVG(vending_hourly_avg) AS vending_monthly_avg FROM ( SELECT dt, yr, mo, hr, AVG(coffee) AS coffee_hourly_avg, AVG(printer) AS printer_hourly_avg, AVG(projector) AS projector_hourly_avg, AVG(vending) AS vending_hourly_avg FROM ( SELECT CAST(log_time AS DATE) AS dt, toYear(log_time) AS yr, EXTRACT(MONTH FROM log_time) AS mo, toHour(log_time) AS hr, CASE WHEN device_name LIKE 'coffee%' THEN event_value END AS coffee, CASE WHEN device_name LIKE 'printer%' THEN event_value END AS printer, CASE WHEN device_name LIKE 'projector%' THEN event_value END AS projector, CASE WHEN device_name LIKE 'vending%' THEN event_value END AS vending FROM mgbench.logs3 WHERE device_type = 'meter' ) AS r GROUP BY dt, yr, mo, hr ) AS s GROUP BY yr, mo ORDER BY yr, mo;",
            "comment": " -- Q3.6: For each device category, what are the monthly power consumption metrics?",
        },
        {
            "query": "SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE F_YEAR = 1993 AND LO_DISCOUNT BETWEEN 1 AND 3 AND LO_QUANTITY < 25;",
            "comment": " -- Q1.1",
        },
        {
            "query": "SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE toYYYYMM(LO_ORDERDATE) = 199401 AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;",
            "comment": " -- Q1.2",
        },
        {
            "query": "SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE toRelativeWeekNum(LO_ORDERDATE) - toRelativeWeekNum(toDate('1994-01-01')) = 6 AND F_YEAR = 1994 AND LO_DISCOUNT BETWEEN 5 AND 7 AND LO_QUANTITY BETWEEN 26 AND 35;",
            "comment": " -- Q1.3",
        },
        {
            "query": "SELECT sum(LO_REVENUE), F_YEAR AS year, P_BRAND FROM lineorder_flat WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA' GROUP BY year, P_BRAND ORDER BY year, P_BRAND;",
            "comment": " -- Q2.1",
        },
        {
            "query": "SELECT sum(LO_REVENUE), F_YEAR AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND >= 'MFGR#2221' AND P_BRAND <= 'MFGR#2228' AND S_REGION = 'ASIA' GROUP BY year, P_BRAND ORDER BY year, P_BRAND;",
            "comment": " -- Q2.2",
        },
        {
            "query": "SELECT sum(LO_REVENUE), F_YEAR AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE' GROUP BY year, P_BRAND ORDER BY year, P_BRAND;",
            "comment": " -- Q2.3",
        },
        {
            "query": "SELECT C_NATION, S_NATION, F_YEAR AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_REGION = 'ASIA' AND S_REGION = 'ASIA' AND year >= 1992 AND year <= 1997 GROUP BY C_NATION, S_NATION, year ORDER BY year ASC, revenue DESC;",
            "comment": " -- Q3.1",
        },
        {
            "query": "SELECT C_CITY, S_CITY, F_YEAR AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_NATION = 'UNITED STATES' AND S_NATION = 'UNITED STATES' AND year >= 1992 AND year <= 1997 GROUP BY C_CITY, S_CITY, year ORDER BY year ASC, revenue DESC;",
            "comment": " -- Q3.2",
        },
        {
            "query": "SELECT C_CITY, S_CITY, F_YEAR AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5') AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5') AND year >= 1992 AND year <= 1997 GROUP BY C_CITY, S_CITY, year ORDER BY year ASC, revenue DESC;",
            "comment": " -- Q3.3",
        },
        {
            "query": "SELECT C_CITY, S_CITY, F_YEAR AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5') AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5') AND toYYYYMM(LO_ORDERDATE) = 199712 GROUP BY C_CITY, S_CITY, year ORDER BY year ASC, revenue DESC;",
            "comment": " -- Q3.4",
        },
        {
            "query": "SELECT F_YEAR AS year, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY year, C_NATION ORDER BY year ASC, C_NATION ASC;",
            "comment": " -- Q4.1",
        },
        {
            "query": "SELECT F_YEAR AS year, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (year = 1997 OR year = 1998) AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY year, S_NATION, P_CATEGORY ORDER BY year ASC, S_NATION ASC, P_CATEGORY ASC;",
            "comment": " -- Q4.2",
        },
        {
            "query": "SELECT F_YEAR AS year, S_CITY, P_BRAND, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE S_NATION = 'UNITED STATES' AND (year = 1997 OR year = 1998) AND P_CATEGORY = 'MFGR#14' GROUP BY year, S_CITY, P_BRAND ORDER BY year ASC, S_CITY ASC, P_BRAND ASC;",
            "comment": " -- Q4.",
        },
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
            "query": "SELECT    URL,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT DontCountHits    AND NOT Refresh    AND notEmpty(URL) GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT    Title,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT DontCountHits    AND NOT Refresh    AND notEmpty(Title) GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
            "comment": "",
        },
        {
            "query": "SELECT    URL,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND IsLink    AND NOT IsDownload GROUP BY URL ORDER BY PageViews DESC LIMIT 1000",
            "comment": "",
        },
        {
            "query": "SELECT    TraficSourceID,    SearchEngineID,    AdvEngineID,    ((SearchEngineID = 0 AND AdvEngineID = 0) ? Referer : '') AS Src,    URL AS Dst,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh GROUP BY    TraficSourceID,    SearchEngineID,    AdvEngineID,    Src,    Dst ORDER BY PageViews DESC LIMIT 1000",
            "comment": "",
        },
        {
            "query": "SELECT    URLHash,    EventDate,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND TraficSourceID IN (-1, 6)    AND RefererHash = halfMD5('http://yandex.ru/') GROUP BY    URLHash,    EventDate ORDER BY PageViews DESC LIMIT 100",
            "comment": "",
        },
        {
            "query": "SELECT    WindowClientWidth,    WindowClientHeight,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-31')    AND NOT Refresh    AND NOT DontCountHits    AND URLHash = halfMD5('http://yandex.ru/') GROUP BY    WindowClientWidth,    WindowClientHeight ORDER BY PageViews DESC LIMIT 10000;",
            "comment": "",
        },
        {
            "query": "SELECT    toStartOfMinute(EventTime) AS Minute,    count() AS PageViews FROM hits WHERE    CounterID = 62    AND EventDate >= toDate('2013-07-01')    AND EventDate <= toDate('2013-07-02')    AND NOT Refresh    AND NOT DontCountHits GROUP BY    Minute ORDER BY Minute;",
            "comment": "",
        }
    ];
