-- Tags: no-tsan, no-replicated-database, no-parallel
-- Tag no-replicated-database: Fails due to additional replicas or shards

DROP TABLE IF EXISTS fixed_granularity_table;

CREATE TABLE fixed_granularity_table (`WatchID` UInt64, `JavaEnable` UInt8, `Title` String, `GoodEvent` Int16, `EventTime` DateTime, `EventDate` Date, `CounterID` UInt32, `ClientIP` UInt32, `ClientIP6` FixedString(16), `RegionID` UInt32, `UserID` UInt64, `CounterClass` Int8, `OS` UInt8, `UserAgent` UInt8, `URL` String, `Referer` String, `URLDomain` String, `RefererDomain` String, `Refresh` UInt8, `IsRobot` UInt8, `RefererCategories` Array(UInt16), `URLCategories` Array(UInt16), `URLRegions` Array(UInt32), `RefererRegions` Array(UInt32), `ResolutionWidth` UInt16, `ResolutionHeight` UInt16, `ResolutionDepth` UInt8, `FlashMajor` UInt8, `FlashMinor` UInt8, `FlashMinor2` String, `NetMajor` UInt8, `NetMinor` UInt8, `UserAgentMajor` UInt16, `UserAgentMinor` FixedString(2), `CookieEnable` UInt8, `JavascriptEnable` UInt8, `IsMobile` UInt8, `MobilePhone` UInt8, `MobilePhoneModel` String, `Params` String, `IPNetworkID` UInt32, `TraficSourceID` Int8, `SearchEngineID` UInt16, `SearchPhrase` String, `AdvEngineID` UInt8, `IsArtifical` UInt8, `WindowClientWidth` UInt16, `WindowClientHeight` UInt16, `ClientTimeZone` Int16, `ClientEventTime` DateTime, `SilverlightVersion1` UInt8, `SilverlightVersion2` UInt8, `SilverlightVersion3` UInt32, `SilverlightVersion4` UInt16, `PageCharset` String, `CodeVersion` UInt32, `IsLink` UInt8, `IsDownload` UInt8, `IsNotBounce` UInt8, `FUniqID` UInt64, `HID` UInt32, `IsOldCounter` UInt8, `IsEvent` UInt8, `IsParameter` UInt8, `DontCountHits` UInt8, `WithHash` UInt8, `HitColor` FixedString(1), `UTCEventTime` DateTime, `Age` UInt8, `Sex` UInt8, `Income` UInt8, `Interests` UInt16, `Robotness` UInt8, `GeneralInterests` Array(UInt16), `RemoteIP` UInt32, `RemoteIP6` FixedString(16), `WindowName` Int32, `OpenerName` Int32, `HistoryLength` Int16, `BrowserLanguage` FixedString(2), `BrowserCountry` FixedString(2), `SocialNetwork` String, `SocialAction` String, `HTTPError` UInt16, `SendTiming` Int32, `DNSTiming` Int32, `ConnectTiming` Int32, `ResponseStartTiming` Int32, `ResponseEndTiming` Int32, `FetchTiming` Int32, `RedirectTiming` Int32, `DOMInteractiveTiming` Int32, `DOMContentLoadedTiming` Int32, `DOMCompleteTiming` Int32, `LoadEventStartTiming` Int32, `LoadEventEndTiming` Int32, `NSToDOMContentLoadedTiming` Int32, `FirstPaintTiming` Int32, `RedirectCount` Int8, `SocialSourceNetworkID` UInt8, `SocialSourcePage` String, `ParamPrice` Int64, `ParamOrderID` String, `ParamCurrency` FixedString(3), `ParamCurrencyID` UInt16, `GoalsReached` Array(UInt32), `OpenstatServiceName` String, `OpenstatCampaignID` String, `OpenstatAdID` String, `OpenstatSourceID` String, `UTMSource` String, `UTMMedium` String, `UTMCampaign` String, `UTMContent` String, `UTMTerm` String, `FromTag` String, `HasGCLID` UInt8, `RefererHash` UInt64, `URLHash` UInt64, `CLID` UInt32, `YCLID` UInt64, `ShareService` String, `ShareURL` String, `ShareTitle` String, `ParsedParams.Key1` Array(String), `ParsedParams.Key2` Array(String), `ParsedParams.Key3` Array(String), `ParsedParams.Key4` Array(String), `ParsedParams.Key5` Array(String), `ParsedParams.ValueDouble` Array(Float64), `IslandID` FixedString(16), `RequestNum` UInt32, `RequestTry` UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192, index_granularity_bytes=0, min_bytes_for_wide_part = 0; -- looks like default table before update

INSERT INTO fixed_granularity_table SELECT * FROM test.hits LIMIT 10; -- should still have non adaptive granularity
INSERT INTO fixed_granularity_table SELECT * FROM test.hits LIMIT 10;

-- We have removed testing of OPTIMIZE because it's too heavy on very slow builds (debug + coverage + thread fuzzer with sleeps)
-- OPTIMIZE TABLE fixed_granularity_table FINAL; -- and even after optimize

DETACH TABLE fixed_granularity_table;
ATTACH TABLE fixed_granularity_table;

ALTER TABLE fixed_granularity_table DETACH PARTITION 201403;
ALTER TABLE fixed_granularity_table ATTACH PARTITION 201403;

SELECT count() from fixed_granularity_table;

DROP TABLE IF EXISTS fixed_granularity_table;
