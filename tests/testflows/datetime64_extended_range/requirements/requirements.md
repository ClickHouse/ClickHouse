# SRS-010 ClickHouse DateTime64 Extended Range
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
  * 3.1 [SRS](#srs)
  * 3.2 [Normal Date Range](#normal-date-range)
  * 3.3 [Extended Date Range](#extended-date-range)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
      * 4.1.0.1 [RQ.SRS-010.DateTime64.ExtendedRange](#rqsrs-010datetime64extendedrange)
      * 4.1.0.2 [RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start](#rqsrs-010datetime64extendedrangenormalrangestart)
      * 4.1.0.3 [RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start.BeforeEpochForTimeZone](#rqsrs-010datetime64extendedrangenormalrangestartbeforeepochfortimezone)
      * 4.1.0.4 [RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End](#rqsrs-010datetime64extendedrangenormalrangeend)
      * 4.1.0.5 [RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End.AfterEpochForTimeZone](#rqsrs-010datetime64extendedrangenormalrangeendafterepochfortimezone)
      * 4.1.0.6 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions](#rqsrs-010datetime64extendedrangetypeconversionfunctions)
      * 4.1.0.7 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions](#rqsrs-010datetime64extendedrangedatesandtimesfunctions)
      * 4.1.0.8 [RQ.SRS-010.DateTime64.ExtendedRange.TimeZones](#rqsrs-010datetime64extendedrangetimezones)
      * 4.1.0.9 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime](#rqsrs-010datetime64extendedrangenonexistenttime)
      * 4.1.0.10 [RQ.SRS-010.DateTime64.ExtendedRange.Comparison](#rqsrs-010datetime64extendedrangecomparison)
      * 4.1.0.11 [RQ.SRS-010.DateTime64.ExtendedRange.SpecificTimestamps](#rqsrs-010datetime64extendedrangespecifictimestamps)
  * 4.2 [Specific](#specific)
      * 4.2.0.1 [RQ.SRS-010.DateTime64.ExtendedRange.Start](#rqsrs-010datetime64extendedrangestart)
      * 4.2.0.2 [RQ.SRS-010.DateTime64.ExtendedRange.End](#rqsrs-010datetime64extendedrangeend)
      * 4.2.0.3 [Non-Existent Time](#non-existent-time)
        * 4.2.0.3.1 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidDate](#rqsrs-010datetime64extendedrangenonexistenttimeinvaliddate)
        * 4.2.0.3.2 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidTime](#rqsrs-010datetime64extendedrangenonexistenttimeinvalidtime)
        * 4.2.0.3.3 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.TimeZoneSwitch](#rqsrs-010datetime64extendedrangenonexistenttimetimezoneswitch)
        * 4.2.0.3.4 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime](#rqsrs-010datetime64extendedrangenonexistenttimedaylightsavingtime)
        * 4.2.0.3.5 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime.Disappeared](#rqsrs-010datetime64extendedrangenonexistenttimedaylightsavingtimedisappeared)
        * 4.2.0.3.6 [RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.LeapSeconds](#rqsrs-010datetime64extendedrangenonexistenttimeleapseconds)
      * 4.2.0.4 [Dates And Times Functions](#dates-and-times-functions)
        * 4.2.0.4.1 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTimeZone](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstotimezone)
        * 4.2.0.4.2 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYear](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoyear)
        * 4.2.0.4.3 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toQuarter](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoquarter)
        * 4.2.0.4.4 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonth](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstomonth)
        * 4.2.0.4.5 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfYear](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstodayofyear)
        * 4.2.0.4.6 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfMonth](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstodayofmonth)
        * 4.2.0.4.7 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfWeek](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstodayofweek)
        * 4.2.0.4.8 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toHour](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstohour)
        * 4.2.0.4.9 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMinute](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstominute)
        * 4.2.0.4.10 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toSecond](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstosecond)
        * 4.2.0.4.11 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toUnixTimestamp](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstounixtimestamp)
        * 4.2.0.4.12 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfYear](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofyear)
        * 4.2.0.4.13 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfISOYear](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofisoyear)
        * 4.2.0.4.14 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfQuarter](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofquarter)
        * 4.2.0.4.15 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMonth](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofmonth)
        * 4.2.0.4.16 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonday](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstomonday)
        * 4.2.0.4.17 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfWeek](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofweek)
        * 4.2.0.4.18 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfDay](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofday)
        * 4.2.0.4.19 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfHour](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofhour)
        * 4.2.0.4.20 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMinute](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofminute)
        * 4.2.0.4.21 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfSecond](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofsecond)
        * 4.2.0.4.22 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFiveMinute](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartoffiveminute)
        * 4.2.0.4.23 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfTenMinutes](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartoftenminutes)
        * 4.2.0.4.24 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFifteenMinutes](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartoffifteenminutes)
        * 4.2.0.4.25 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfInterval](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstostartofinterval)
        * 4.2.0.4.26 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTime](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstotime)
        * 4.2.0.4.27 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeYearNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativeyearnum)
        * 4.2.0.4.28 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeQuarterNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativequarternum)
        * 4.2.0.4.29 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMonthNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativemonthnum)
        * 4.2.0.4.30 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeWeekNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativeweeknum)
        * 4.2.0.4.31 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeDayNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativedaynum)
        * 4.2.0.4.32 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeHourNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativehournum)
        * 4.2.0.4.33 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMinuteNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativeminutenum)
        * 4.2.0.4.34 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeSecondNum](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstorelativesecondnum)
        * 4.2.0.4.35 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOYear](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoisoyear)
        * 4.2.0.4.36 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOWeek](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoisoweek)
        * 4.2.0.4.37 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toWeek](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoweek)
        * 4.2.0.4.38 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYearWeek](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoyearweek)
        * 4.2.0.4.39 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.now](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsnow)
        * 4.2.0.4.40 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.today](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoday)
        * 4.2.0.4.41 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.yesterday](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsyesterday)
        * 4.2.0.4.42 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlot](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstimeslot)
        * 4.2.0.4.43 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMM](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoyyyymm)
        * 4.2.0.4.44 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDD](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoyyyymmdd)
        * 4.2.0.4.45 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDDhhmmss](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstoyyyymmddhhmmss)
        * 4.2.0.4.46 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addYears](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddyears)
        * 4.2.0.4.47 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMonths](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddmonths)
        * 4.2.0.4.48 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addWeeks](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddweeks)
        * 4.2.0.4.49 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addDays](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsadddays)
        * 4.2.0.4.50 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addHours](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddhours)
        * 4.2.0.4.51 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMinutes](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddminutes)
        * 4.2.0.4.52 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addSeconds](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddseconds)
        * 4.2.0.4.53 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addQuarters](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsaddquarters)
        * 4.2.0.4.54 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractYears](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractyears)
        * 4.2.0.4.55 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMonths](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractmonths)
        * 4.2.0.4.56 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractWeeks](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractweeks)
        * 4.2.0.4.57 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractDays](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractdays)
        * 4.2.0.4.58 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractHours](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtracthours)
        * 4.2.0.4.59 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMinutes](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractminutes)
        * 4.2.0.4.60 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractSeconds](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractseconds)
        * 4.2.0.4.61 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractQuarters](#rqsrs-010datetime64extendedrangedatesandtimesfunctionssubtractquarters)
        * 4.2.0.4.62 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.dateDiff](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsdatediff)
        * 4.2.0.4.63 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlots](#rqsrs-010datetime64extendedrangedatesandtimesfunctionstimeslots)
        * 4.2.0.4.64 [RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.formatDateTime](#rqsrs-010datetime64extendedrangedatesandtimesfunctionsformatdatetime)
    * 4.2.1 [Type Conversion Functions](#type-conversion-functions)
        * 4.2.1.4.1 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toInt(8|16|32|64|128|256)](#rqsrs-010datetime64extendedrangetypeconversionfunctionstoint8163264128256)
        * 4.2.1.4.2 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUInt(8|16|32|64|256)](#rqsrs-010datetime64extendedrangetypeconversionfunctionstouint8163264256)
        * 4.2.1.4.3 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toFloat(32|64)](#rqsrs-010datetime64extendedrangetypeconversionfunctionstofloat3264)
        * 4.2.1.4.4 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDate](#rqsrs-010datetime64extendedrangetypeconversionfunctionstodate)
        * 4.2.1.4.5 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime](#rqsrs-010datetime64extendedrangetypeconversionfunctionstodatetime)
        * 4.2.1.4.6 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64](#rqsrs-010datetime64extendedrangetypeconversionfunctionstodatetime64)
        * 4.2.1.4.7 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64.FromString.MissingTime](#rqsrs-010datetime64extendedrangetypeconversionfunctionstodatetime64fromstringmissingtime)
        * 4.2.1.4.8 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDecimal(32|64|128|256)](#rqsrs-010datetime64extendedrangetypeconversionfunctionstodecimal3264128256)
        * 4.2.1.4.9 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toString](#rqsrs-010datetime64extendedrangetypeconversionfunctionstostring)
        * 4.2.1.4.10 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.CAST(x,T)](#rqsrs-010datetime64extendedrangetypeconversionfunctionscastxt)
        * 4.2.1.4.11 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Milli](#rqsrs-010datetime64extendedrangetypeconversionfunctionstounixtimestamp64milli)
        * 4.2.1.4.12 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Micro](#rqsrs-010datetime64extendedrangetypeconversionfunctionstounixtimestamp64micro)
        * 4.2.1.4.13 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Nano](#rqsrs-010datetime64extendedrangetypeconversionfunctionstounixtimestamp64nano)
        * 4.2.1.4.14 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Milli](#rqsrs-010datetime64extendedrangetypeconversionfunctionsfromunixtimestamp64milli)
        * 4.2.1.4.15 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Micro](#rqsrs-010datetime64extendedrangetypeconversionfunctionsfromunixtimestamp64micro)
        * 4.2.1.4.16 [RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Nano](#rqsrs-010datetime64extendedrangetypeconversionfunctionsfromunixtimestamp64nano)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

This document will cover requirements to support extended range for the [DateTime64] data type
that is outside the normal **1970** (1970-01-02 00:00:00 UTC) to **2105** (2105-12-31 23:59:59.99999 UTC) date range.

## Terminology

### SRS

Software Requirements Specification

### Normal Date Range

**1970** `1970-01-02T00:00:00.000000` to **2105** `2105-12-31T23:59:59.99999`

### Extended Date Range

**1925** `1925-01-01T00:00:00.000000` to **2238** `2238-12-31 23:59:59.99999`

## Requirements

### Generic

##### RQ.SRS-010.DateTime64.ExtendedRange
version: 1.0

[ClickHouse] SHALL support extended range for the [DateTime64] data type that includes dates from the year **1925** to **2238**.

##### RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start
version: 1.0

[ClickHouse] SHALL support proper time handling around the normal date range that starts at `1970-01-01 00:00:00.000`
expressed using the [ISO 8601 format].

##### RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start.BeforeEpochForTimeZone
version: 1.0

[ClickHouse] SHALL support proper time handling around the start of the [normal date range]
when this time for the time zone is before the start of the [normal date range].

##### RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End
version: 1.0

[ClickHouse] SHALL support proper time handling around the normal date range that ends at `2105-12-31T23:59:59.99999`
expressed using the [ISO 8601 format].

##### RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End.AfterEpochForTimeZone
version: 1.0

[ClickHouse] SHALL support proper time handling around the end of the [normal date range]
when this time for the time zone is after the end of the [normal date range].

##### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions
version: 1.0

[ClickHouse] SHALL support proper conversion to and from [DateTime64] data type from other data types.

##### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions
version: 1.0

[ClickHouse] SHALL support correct operation of the [Dates and Times Functions] with the [DateTime64] data type
when it stores dates within the [normal date range] and the [extended date range].

##### RQ.SRS-010.DateTime64.ExtendedRange.TimeZones
version: 1.0

[ClickHouse] SHALL support correct operation with the [DateTime64] extended range data type
when combined with a supported time zone.

##### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime
version: 1.0

[ClickHouse] SHALL support proper handling of non-existent times when using [DateTime64] extended range data type.

##### RQ.SRS-010.DateTime64.ExtendedRange.Comparison
version: 1.0

[ClickHouse] SHALL support proper handling of time comparison when using [DateTime64] extended range data type.
For example, `SELECT toDateTime64('2019-05-05 20:20:12.050', 3) < now()`.

##### RQ.SRS-010.DateTime64.ExtendedRange.SpecificTimestamps
version: 1.0

[ClickHouse] SHALL properly work with the following timestamps in all supported timezones:
```
[9961200,73476000,325666800,354675600,370400400,386125200,388566010,401850000,417574811,496803600,528253200,624423614,636516015,671011200,717555600,752047218,859683600,922582800,1018173600,1035705600,1143334800,1162105223,1174784400,1194156000,1206838823,1224982823,1236495624,1319936400,1319936424,1425798025,1459040400,1509872400,2090451627,2140668000]
```


### Specific

##### RQ.SRS-010.DateTime64.ExtendedRange.Start
version: 1.0

[ClickHouse] SHALL support extended range for the [DateTime64] data type that starts at `1925-01-01T00:00:00.000000`
expressed using the [ISO 8601 format].

##### RQ.SRS-010.DateTime64.ExtendedRange.End
version: 1.0

[ClickHouse] SHALL support extended range for the [DateTime64] data type that ends at `2238-12-31T23:59:59.999999`
expressed using the [ISO 8601 format].

##### Non-Existent Time

###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidDate
version: 1.0

[ClickHouse] SHALL support proper handling of invalid dates when using [DateTime64] extended range data type,
such as:

* `YYYY-04-31, YYYY-06-31, YYYY-09-31, YYYY-11-31`
* `1990-02-30 00:00:02`

###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidTime
version: 1.0

[ClickHouse] SHALL support proper handling of invalid time for a timezone
when using [DateTime64] extended range data type, for example,

* `2002-04-07 02:30:00` never happened at all in the US/Eastern timezone ([Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime))


###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.TimeZoneSwitch
version: 1.0

[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type
when the invalid time is caused when *countries switch timezone definitions with no
daylight savings time switch* [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime).

>
> For example, in 1915 Warsaw switched from Warsaw time to Central European time with
> no daylight savings transition. So at the stroke of midnight on August 5th 1915 the clocks
> were wound back 24 minutes creating an ambiguous time period that cannot be specified without
> referring to the timezone abbreviation or the actual UTC offset. In this case midnight happened twice,
> neither time during a daylight saving time period. pytz handles this transition by treating the ambiguous
> period before the switch as daylight savings time, and the ambiguous period after as standard time.
>
> [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime)

###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime
version: 1.0

[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type
when for a given timezone time switches from standard to daylight saving.

> For example, in the US/Eastern timezone on the last Sunday morning in October, the following sequence happens:
>
> 01:00 EDT occurs
> 1 hour later, instead of 2:00am the clock is turned back 1 hour and 01:00 happens again (this time 01:00 EST)
> In fact, every instant between 01:00 and 02:00 occurs twice.
> [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime)

###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime.Disappeared
version: 1.0

[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type
for a given timezone when transition from the standard to daylight saving time causes an hour to disappear.

Expected behavior: if DateTime64 initialized by a skipped time value, it is being treated as DST and resulting value will be an hour earlier, e.g. `SELECT toDateTime64('2020-03-08 02:34:00', 0, 'America/Denver')` returns `2020-03-08 01:34:00`.

###### RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.LeapSeconds
version: 1.0

[ClickHouse] SHALL support proper handling of leap seconds adjustments when using [DateTime64] extended range data type.

##### Dates And Times Functions

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTimeZone
version: 1.0

[ClickHouse] SHALL support correct operation of the [toTimeZone](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#totimezone)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYear
version: 1.0

[ClickHouse] SHALL support correct operation of the [toYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyear)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toQuarter
version: 1.0

[ClickHouse] SHALL support correct operation of the [toQuarter](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toquarter)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonth
version: 1.0

[ClickHouse] SHALL support correct operation of the [toMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tomonth)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfYear
version: 1.0

[ClickHouse] SHALL support correct operation of the [toDayOfYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofyear)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfMonth
version: 1.0

[ClickHouse] SHALL support correct operation of the [toDayOfMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofmonth)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfWeek
version: 1.0

[ClickHouse] SHALL support correct operation of the [toDayOfWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofweek)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toHour
version: 1.0

[ClickHouse] SHALL support correct operation of the [toHour](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tohour)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMinute
version: 1.0

[ClickHouse] SHALL support correct operation of the [toMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tominute)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toSecond
version: 1.0

[ClickHouse] SHALL support correct operation of the [toSecond](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tosecond)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toUnixTimestamp
version: 1.0

[ClickHouse] SHALL support correct operation of the [toUnitTimestamp](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#to-unix-timestamp)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].
Timestamp value expected to be negative when DateTime64 value is prior to `1970-01-01` and positine otherwise.

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfYear
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofyear)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfISOYear
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfISOYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofisoyear)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfQuarter
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfQuarter](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofquarter)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMonth
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofmonth)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonday
version: 1.0

[ClickHouse] SHALL support correct operation of the [toMonday](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tomonday)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfWeek
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofweektmode)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfDay
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfDay](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofday)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfHour
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfHour](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofhour)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMinute
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofminute)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfSecond
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfSecond](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofsecond)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFiveMinute
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfFiveMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoffiveminute)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfTenMinutes
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfTenMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoftenminutes)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFifteenMinutes
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfFifteenMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoffifteenminutes)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfInterval
version: 1.0

[ClickHouse] SHALL support correct operation of the [toStartOfInterval](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofintervaltime-or-data-interval-x-unit-time-zone)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].
More detailed description can be found [here](https://github.com/ClickHouse/ClickHouse/issues/1201).

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTime
version: 1.0

[ClickHouse] SHALL support correct operation of the [toTime](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#totime)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeYearNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeYearNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeyearnum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeQuarterNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeQuarterNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativequarternum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMonthNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeMonthNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativemonthnum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeWeekNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeWeekNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeweeknum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeDayNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeDayNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativedaynum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeHourNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeHourNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativehournum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMinuteNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeMinuteNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeminutenum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeSecondNum
version: 1.0

[ClickHouse] SHALL support correct operation of the [toRelativeSecondNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativesecondnum)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOYear
version: 1.0

[ClickHouse] SHALL support correct operation of the [toISOYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toisoyear)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOWeek
version: 1.0

[ClickHouse] SHALL support correct operation of the [toISOWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toisoweek)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toWeek
version: 1.0

[ClickHouse] SHALL support correct operation of the [toWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toweekdatemode)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYearWeek
version: 1.0

[ClickHouse] SHALL support correct operation of the [toYearWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyearweekdatemode)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.now
version: 1.0

[ClickHouse] SHALL support conversion of output from the [now](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#now)
function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.today
version: 1.0

[ClickHouse] SHALL support conversion of output from the [today](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#today)
function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.yesterday
version: 1.0

[ClickHouse] SHALL support conversion of output from the [yesterday](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#yesterday)
function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlot
version: 1.0

[ClickHouse] SHALL support conversion of output from the [timeSlot](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#timeslot)
function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMM
version: 1.0

[ClickHouse] SHALL support correct operation of the [toYYYYMM](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymm)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDD
version: 1.0

[ClickHouse] SHALL support correct operation of the [toYYYYMMDD](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymmdd)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDDhhmmss
version: 1.0

[ClickHouse] SHALL support correct operation of the [toYYYYMMDDhhmmss](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymmddhhmmss)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addYears
version: 1.0

[ClickHouse] SHALL support correct operation of the [addYears](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMonths
version: 1.0

[ClickHouse] SHALL support correct operation of the [addMonths](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addWeeks
version: 1.0

[ClickHouse] SHALL support correct operation of the [addWeeks](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addDays
version: 1.0

[ClickHouse] SHALL support correct operation of the [addDays](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addHours
version: 1.0

[ClickHouse] SHALL support correct operation of the [addHours](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMinutes
version: 1.0

[ClickHouse] SHALL support correct operation of the [addMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addSeconds
version: 1.0

[ClickHouse] SHALL support correct operation of the [addSeconds](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addQuarters
version: 1.0

[ClickHouse] SHALL support correct operation of the [addQuarters](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractYears
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractYears](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMonths
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractMonths](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractWeeks
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractWeeks](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractDays
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractDays](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].


###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractHours
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractHours](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMinutes
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractSeconds
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractSeconds](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractQuarters
version: 1.0

[ClickHouse] SHALL support correct operation of the [subtractQuarters](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.dateDiff
version: 1.0

[ClickHouse] SHALL support correct operation of the [dateDiff](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#datediff)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlots
version: 1.0

[ClickHouse] SHALL support correct operation of the [timeSlots](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#timeslotsstarttime-duration-size)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.formatDateTime
version: 1.0

[ClickHouse] SHALL support correct operation of the [formatDateTime](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#formatdatetime)
function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].


#### Type Conversion Functions

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toInt(8|16|32|64|128|256)
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to integer types using [toInt(8|16|32|64|128|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#toint8163264128256) functions.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUInt(8|16|32|64|256)
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to unsigned integer types using [toUInt(8|16|32|64|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#touint8163264256) functions.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toFloat(32|64)
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to float types using [toFloat(32|64)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tofloat3264) functions.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDate
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range]
to the [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date/) type using the [toDate](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todate) function.
This function is ONLY supposed to work in NORMAL RANGE.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to the [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime/) type using the [toDateTime](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todatetime) function.
This function is ONLY supposed to work in NORMAL RANGE.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64
version: 1.0

[ClickHouse] SHALL support correct conversion from the data types supported by the [toDateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/) function
to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64.FromString.MissingTime
version: 1.0

[ClickHouse] SHALL support correct conversion from the [String](https://clickhouse.com/docs/en/sql-reference/data-types/string/)
data type to the [DateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/) data type
when value of the string is missing the `hh:mm-ss.sss` part.
For example, `toDateTime64('2020-01-01', 3)`.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDecimal(32|64|128|256)
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to [Decimal](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/) types using [toDecimal(32|64|128|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todecimal3264128256) functions.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toString
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to the [String](https://clickhouse.com/docs/en/sql-reference/data-types/string/) type using the [toString](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tostring) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.CAST(x,T)
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to one of the supported data type using the [CAST(x,T)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Milli
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Milli](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64milli) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Micro
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Micro](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64micro) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Nano
version: 1.0

[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Nano](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64nano) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Milli
version: 1.0

[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type
to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
using the [fromUnixTimestamp64Milli](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64milli) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Micro
version: 1.0

[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type
to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
using the [fromUnixTimestamp64Micro](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64micro) function.

###### RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Nano
version: 1.0

[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type
to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]
using the [fromUnixTimestamp64Nano](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64nano) function.

## References

* **DateTime64**: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/
* **ISO 8601 format**: https://en.wikipedia.org/wiki/ISO_8601
* **ClickHouse:** https://clickhouse.com
* **GitHub Repository:** https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/datetime64_extended_range/requirements/requirements.md
* **Revision History:** https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/datetime64_extended_range/requirements/requirements.md
* **Git:** https://git-scm.com/

[SRS]: #srs
[normal date range]: #normal-date-range
[extended date range]: #extended-date-range
[Dates and Times Functions]: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/
[DateTime64]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/
[ISO 8601 format]: https://en.wikipedia.org/wiki/ISO_8601
[ClickHouse]: https://clickhouse.com
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/datetime64_extended_range/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/datetime64_extended_range/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
