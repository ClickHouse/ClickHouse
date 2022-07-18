# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.210505.1133630.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_010_DateTime64_ExtendedRange = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support extended range for the [DateTime64] data type that includes dates from the year **1925** to **2238**.\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.1",
)

RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper time handling around the normal date range that starts at `1970-01-01 00:00:00.000`\n"
        "expressed using the [ISO 8601 format].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.2",
)

RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start_BeforeEpochForTimeZone = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start.BeforeEpochForTimeZone",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper time handling around the start of the [normal date range]\n"
        "when this time for the time zone is before the start of the [normal date range].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.3",
)

RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper time handling around the normal date range that ends at `2105-12-31T23:59:59.99999`\n"
        "expressed using the [ISO 8601 format].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.4",
)

RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End_AfterEpochForTimeZone = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End.AfterEpochForTimeZone",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper time handling around the end of the [normal date range]\n"
        "when this time for the time zone is after the end of the [normal date range].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.5",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper conversion to and from [DateTime64] data type from other data types.\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.6",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [Dates and Times Functions] with the [DateTime64] data type\n"
        "when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.7",
)

RQ_SRS_010_DateTime64_ExtendedRange_TimeZones = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TimeZones",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation with the [DateTime64] extended range data type\n"
        "when combined with a supported time zone.\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.8",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of non-existent times when using [DateTime64] extended range data type.\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.9",
)

RQ_SRS_010_DateTime64_ExtendedRange_Comparison = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.Comparison",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of time comparison when using [DateTime64] extended range data type.\n"
        "For example, `SELECT toDateTime64('2019-05-05 20:20:12.050', 3) < now()`.\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.10",
)

RQ_SRS_010_DateTime64_ExtendedRange_SpecificTimestamps = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.SpecificTimestamps",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL properly work with the following timestamps in all supported timezones:\n"
        "```\n"
        "[9961200,73476000,325666800,354675600,370400400,386125200,388566010,401850000,417574811,496803600,528253200,624423614,636516015,671011200,717555600,752047218,859683600,922582800,1018173600,1035705600,1143334800,1162105223,1174784400,1194156000,1206838823,1224982823,1236495624,1319936400,1319936424,1425798025,1459040400,1509872400,2090451627,2140668000]\n"
        "```\n"
        "\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.1.0.11",
)

RQ_SRS_010_DateTime64_ExtendedRange_Start = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.Start",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support extended range for the [DateTime64] data type that starts at `1925-01-01T00:00:00.000000`\n"
        "expressed using the [ISO 8601 format].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.2.0.1",
)

RQ_SRS_010_DateTime64_ExtendedRange_End = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.End",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support extended range for the [DateTime64] data type that ends at `2238-12-31T23:59:59.999999`\n"
        "expressed using the [ISO 8601 format].\n"
        "\n"
    ),
    link=None,
    level=4,
    num="4.2.0.2",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidDate = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidDate",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of invalid dates when using [DateTime64] extended range data type,\n"
        "such as:\n"
        "\n"
        "* `YYYY-04-31, YYYY-06-31, YYYY-09-31, YYYY-11-31`\n"
        "* `1990-02-30 00:00:02`\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.1",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of invalid time for a timezone\n"
        "when using [DateTime64] extended range data type, for example,\n"
        "\n"
        "* `2002-04-07 02:30:00` never happened at all in the US/Eastern timezone ([Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime))\n"
        "\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.2",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_TimeZoneSwitch = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.TimeZoneSwitch",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type\n"
        "when the invalid time is caused when *countries switch timezone definitions with no\n"
        "daylight savings time switch* [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime).\n"
        "\n"
        ">\n"
        "> For example, in 1915 Warsaw switched from Warsaw time to Central European time with\n"
        "> no daylight savings transition. So at the stroke of midnight on August 5th 1915 the clocks\n"
        "> were wound back 24 minutes creating an ambiguous time period that cannot be specified without\n"
        "> referring to the timezone abbreviation or the actual UTC offset. In this case midnight happened twice,\n"
        "> neither time during a daylight saving time period. pytz handles this transition by treating the ambiguous\n"
        "> period before the switch as daylight savings time, and the ambiguous period after as standard time.\n"
        ">\n"
        "> [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime)\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.3",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type\n"
        "when for a given timezone time switches from standard to daylight saving.\n"
        "\n"
        "> For example, in the US/Eastern timezone on the last Sunday morning in October, the following sequence happens:\n"
        ">\n"
        "> 01:00 EDT occurs\n"
        "> 1 hour later, instead of 2:00am the clock is turned back 1 hour and 01:00 happens again (this time 01:00 EST)\n"
        "> In fact, every instant between 01:00 and 02:00 occurs twice.\n"
        "> [Stuart Bishop: pytz library](http://pytz.sourceforge.net/#problems-with-localtime)\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.4",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime_Disappeared = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime.Disappeared",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of invalid time when using [DateTime64] extended range data type\n"
        "for a given timezone when transition from the standard to daylight saving time causes an hour to disappear.\n"
        "\n"
        "Expected behavior: if DateTime64 initialized by a skipped time value, it is being treated as DST and resulting value will be an hour earlier, e.g. `SELECT toDateTime64('2020-03-08 02:34:00', 0, 'America/Denver')` returns `2020-03-08 01:34:00`.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.5",
)

RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_LeapSeconds = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.LeapSeconds",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support proper handling of leap seconds adjustments when using [DateTime64] extended range data type.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.3.6",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTimeZone = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTimeZone",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toTimeZone](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#totimezone)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.1",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYear = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYear",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyear)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.2",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toQuarter = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toQuarter",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toQuarter](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toquarter)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.3",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonth = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonth",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tomonth)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.4",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfYear = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfYear",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toDayOfYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofyear)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.5",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfMonth = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfMonth",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toDayOfMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofmonth)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.6",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfWeek = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfWeek",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toDayOfWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#todayofweek)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.7",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toHour = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toHour",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toHour](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tohour)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.8",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMinute = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMinute",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tominute)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.9",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toSecond = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toSecond",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toSecond](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tosecond)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.10",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toUnixTimestamp = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toUnixTimestamp",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toUnitTimestamp](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#to-unix-timestamp)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "Timestamp value expected to be negative when DateTime64 value is prior to `1970-01-01` and positine otherwise.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.11",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfYear = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfYear",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofyear)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.12",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfISOYear = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfISOYear",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfISOYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofisoyear)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.13",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfQuarter = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfQuarter",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfQuarter](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofquarter)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.14",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMonth = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMonth",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfMonth](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofmonth)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.15",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonday = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonday",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toMonday](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tomonday)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.16",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfWeek = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfWeek",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofweektmode)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.17",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfDay = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfDay",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfDay](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofday)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.18",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfHour = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfHour",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfHour](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofhour)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.19",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMinute = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMinute",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofminute)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.20",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfSecond = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfSecond",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfSecond](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofsecond)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.21",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFiveMinute = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFiveMinute",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfFiveMinute](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoffiveminute)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.22",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfTenMinutes = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfTenMinutes",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfTenMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoftenminutes)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.23",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFifteenMinutes = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFifteenMinutes",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfFifteenMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartoffifteenminutes)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.24",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfInterval = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfInterval",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toStartOfInterval](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#tostartofintervaltime-or-data-interval-x-unit-time-zone)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "More detailed description can be found [here](https://github.com/ClickHouse/ClickHouse/issues/1201).\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.25",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toTime](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#totime)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.26",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeYearNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeYearNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeYearNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeyearnum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.27",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeQuarterNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeQuarterNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeQuarterNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativequarternum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.28",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMonthNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMonthNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeMonthNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativemonthnum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.29",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeWeekNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeWeekNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeWeekNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeweeknum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.30",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeDayNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeDayNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeDayNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativedaynum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.31",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeHourNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeHourNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeHourNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativehournum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.32",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMinuteNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMinuteNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeMinuteNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativeminutenum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.33",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeSecondNum = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeSecondNum",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toRelativeSecondNum](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#torelativesecondnum)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.34",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOYear = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOYear",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toISOYear](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toisoyear)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.35",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOWeek = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOWeek",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toISOWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toisoweek)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.36",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toWeek = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toWeek",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toweekdatemode)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.37",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYearWeek = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYearWeek",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toYearWeek](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyearweekdatemode)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.38",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_now = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.now",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support conversion of output from the [now](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#now)\n"
        "function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.39",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_today = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.today",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support conversion of output from the [today](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#today)\n"
        "function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.40",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_yesterday = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.yesterday",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support conversion of output from the [yesterday](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#yesterday)\n"
        "function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.41",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlot = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlot",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support conversion of output from the [timeSlot](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#timeslot)\n"
        "function to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.42",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMM = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMM",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toYYYYMM](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymm)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.43",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDD = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDD",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toYYYYMMDD](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymmdd)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.44",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDDhhmmss = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDDhhmmss",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [toYYYYMMDDhhmmss](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#toyyyymmddhhmmss)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.45",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addYears = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addYears",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addYears](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.46",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMonths = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMonths",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addMonths](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.47",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addWeeks = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addWeeks",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addWeeks](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.48",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addDays = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addDays",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addDays](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.49",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addHours = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addHours",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addHours](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.50",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMinutes = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMinutes",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.51",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addSeconds = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addSeconds",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addSeconds](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.52",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addQuarters = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addQuarters",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [addQuarters](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.53",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractYears = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractYears",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractYears](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.54",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMonths = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMonths",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractMonths](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.55",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractWeeks = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractWeeks",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractWeeks](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.56",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractDays = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractDays",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractDays](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.57",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractHours = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractHours",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractHours](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.58",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMinutes = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMinutes",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractMinutes](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.59",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractSeconds = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractSeconds",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractSeconds](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.60",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractQuarters = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractQuarters",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [subtractQuarters](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.61",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_dateDiff = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.dateDiff",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [dateDiff](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#datediff)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.62",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlots = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlots",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [timeSlots](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#timeslotsstarttime-duration-size)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.63",
)

RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_formatDateTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.formatDateTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct operation of the [formatDateTime](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#formatdatetime)\n"
        "function used with the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.0.4.64",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toInt_8_16_32_64_128_256_ = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toInt(8|16|32|64|128|256)",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to integer types using [toInt(8|16|32|64|128|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#toint8163264128256) functions.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.1",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUInt_8_16_32_64_256_ = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUInt(8|16|32|64|256)",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to unsigned integer types using [toUInt(8|16|32|64|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#touint8163264256) functions.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.2",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toFloat_32_64_ = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toFloat(32|64)",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to float types using [toFloat(32|64)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tofloat3264) functions.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.3",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDate = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDate",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range]\n"
        "to the [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date/) type using the [toDate](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todate) function.\n"
        "This function is ONLY supposed to work in NORMAL RANGE.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.4",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to the [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime/) type using the [toDateTime](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todatetime) function.\n"
        "This function is ONLY supposed to work in NORMAL RANGE.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.5",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64 = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion from the data types supported by the [toDateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/) function\n"
        "to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range].\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.6",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64_FromString_MissingTime = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64.FromString.MissingTime",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion from the [String](https://clickhouse.com/docs/en/sql-reference/data-types/string/)\n"
        "data type to the [DateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/) data type\n"
        "when value of the string is missing the `hh:mm-ss.sss` part.\n"
        "For example, `toDateTime64('2020-01-01', 3)`.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.7",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDecimal_32_64_128_256_ = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDecimal(32|64|128|256)",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to [Decimal](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/) types using [toDecimal(32|64|128|256)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#todecimal3264128256) functions.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.8",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toString = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toString",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to the [String](https://clickhouse.com/docs/en/sql-reference/data-types/string/) type using the [toString](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tostring) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.9",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_ = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.CAST(x,T)",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to one of the supported data type using the [CAST(x,T)](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.10",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Milli = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Milli",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Milli](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64milli) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.11",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Micro = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Micro",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Micro](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64micro) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.12",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Nano = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Nano",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion of the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "to the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type using the [toUnixTimestamp64Nano](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#tounixtimestamp64nano) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.13",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Milli = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Milli",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type\n"
        "to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "using the [fromUnixTimestamp64Milli](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64milli) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.14",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Micro = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Micro",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type\n"
        "to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "using the [fromUnixTimestamp64Micro](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64micro) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.15",
)

RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Nano = Requirement(
    name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Nano",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support correct conversion from the [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint/) type\n"
        "to the [DateTime64] data type when it stores dates within the [normal date range] and the [extended date range]\n"
        "using the [fromUnixTimestamp64Nano](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#fromunixtimestamp64nano) function.\n"
        "\n"
    ),
    link=None,
    level=5,
    num="4.2.1.4.16",
)

SRS_010_ClickHouse_DateTime64_Extended_Range = Specification(
    name="SRS-010 ClickHouse DateTime64 Extended Range",
    description=None,
    author=None,
    date=None,
    status=None,
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name="Revision History", level=1, num="1"),
        Heading(name="Introduction", level=1, num="2"),
        Heading(name="Terminology", level=1, num="3"),
        Heading(name="SRS", level=2, num="3.1"),
        Heading(name="Normal Date Range", level=2, num="3.2"),
        Heading(name="Extended Date Range", level=2, num="3.3"),
        Heading(name="Requirements", level=1, num="4"),
        Heading(name="Generic", level=2, num="4.1"),
        Heading(name="RQ.SRS-010.DateTime64.ExtendedRange", level=4, num="4.1.0.1"),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start",
            level=4,
            num="4.1.0.2",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.Start.BeforeEpochForTimeZone",
            level=4,
            num="4.1.0.3",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End",
            level=4,
            num="4.1.0.4",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NormalRange.End.AfterEpochForTimeZone",
            level=4,
            num="4.1.0.5",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions",
            level=4,
            num="4.1.0.6",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions",
            level=4,
            num="4.1.0.7",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TimeZones", level=4, num="4.1.0.8"
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime",
            level=4,
            num="4.1.0.9",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.Comparison",
            level=4,
            num="4.1.0.10",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.SpecificTimestamps",
            level=4,
            num="4.1.0.11",
        ),
        Heading(name="Specific", level=2, num="4.2"),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.Start", level=4, num="4.2.0.1"
        ),
        Heading(name="RQ.SRS-010.DateTime64.ExtendedRange.End", level=4, num="4.2.0.2"),
        Heading(name="Non-Existent Time", level=4, num="4.2.0.3"),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidDate",
            level=5,
            num="4.2.0.3.1",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.InvalidTime",
            level=5,
            num="4.2.0.3.2",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.TimeZoneSwitch",
            level=5,
            num="4.2.0.3.3",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime",
            level=5,
            num="4.2.0.3.4",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.DaylightSavingTime.Disappeared",
            level=5,
            num="4.2.0.3.5",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.NonExistentTime.LeapSeconds",
            level=5,
            num="4.2.0.3.6",
        ),
        Heading(name="Dates And Times Functions", level=4, num="4.2.0.4"),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTimeZone",
            level=5,
            num="4.2.0.4.1",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYear",
            level=5,
            num="4.2.0.4.2",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toQuarter",
            level=5,
            num="4.2.0.4.3",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonth",
            level=5,
            num="4.2.0.4.4",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfYear",
            level=5,
            num="4.2.0.4.5",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfMonth",
            level=5,
            num="4.2.0.4.6",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toDayOfWeek",
            level=5,
            num="4.2.0.4.7",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toHour",
            level=5,
            num="4.2.0.4.8",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMinute",
            level=5,
            num="4.2.0.4.9",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toSecond",
            level=5,
            num="4.2.0.4.10",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toUnixTimestamp",
            level=5,
            num="4.2.0.4.11",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfYear",
            level=5,
            num="4.2.0.4.12",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfISOYear",
            level=5,
            num="4.2.0.4.13",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfQuarter",
            level=5,
            num="4.2.0.4.14",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMonth",
            level=5,
            num="4.2.0.4.15",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toMonday",
            level=5,
            num="4.2.0.4.16",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfWeek",
            level=5,
            num="4.2.0.4.17",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfDay",
            level=5,
            num="4.2.0.4.18",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfHour",
            level=5,
            num="4.2.0.4.19",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfMinute",
            level=5,
            num="4.2.0.4.20",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfSecond",
            level=5,
            num="4.2.0.4.21",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFiveMinute",
            level=5,
            num="4.2.0.4.22",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfTenMinutes",
            level=5,
            num="4.2.0.4.23",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfFifteenMinutes",
            level=5,
            num="4.2.0.4.24",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toStartOfInterval",
            level=5,
            num="4.2.0.4.25",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toTime",
            level=5,
            num="4.2.0.4.26",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeYearNum",
            level=5,
            num="4.2.0.4.27",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeQuarterNum",
            level=5,
            num="4.2.0.4.28",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMonthNum",
            level=5,
            num="4.2.0.4.29",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeWeekNum",
            level=5,
            num="4.2.0.4.30",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeDayNum",
            level=5,
            num="4.2.0.4.31",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeHourNum",
            level=5,
            num="4.2.0.4.32",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeMinuteNum",
            level=5,
            num="4.2.0.4.33",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toRelativeSecondNum",
            level=5,
            num="4.2.0.4.34",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOYear",
            level=5,
            num="4.2.0.4.35",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toISOWeek",
            level=5,
            num="4.2.0.4.36",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toWeek",
            level=5,
            num="4.2.0.4.37",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYearWeek",
            level=5,
            num="4.2.0.4.38",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.now",
            level=5,
            num="4.2.0.4.39",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.today",
            level=5,
            num="4.2.0.4.40",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.yesterday",
            level=5,
            num="4.2.0.4.41",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlot",
            level=5,
            num="4.2.0.4.42",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMM",
            level=5,
            num="4.2.0.4.43",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDD",
            level=5,
            num="4.2.0.4.44",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.toYYYYMMDDhhmmss",
            level=5,
            num="4.2.0.4.45",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addYears",
            level=5,
            num="4.2.0.4.46",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMonths",
            level=5,
            num="4.2.0.4.47",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addWeeks",
            level=5,
            num="4.2.0.4.48",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addDays",
            level=5,
            num="4.2.0.4.49",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addHours",
            level=5,
            num="4.2.0.4.50",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addMinutes",
            level=5,
            num="4.2.0.4.51",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addSeconds",
            level=5,
            num="4.2.0.4.52",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.addQuarters",
            level=5,
            num="4.2.0.4.53",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractYears",
            level=5,
            num="4.2.0.4.54",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMonths",
            level=5,
            num="4.2.0.4.55",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractWeeks",
            level=5,
            num="4.2.0.4.56",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractDays",
            level=5,
            num="4.2.0.4.57",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractHours",
            level=5,
            num="4.2.0.4.58",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractMinutes",
            level=5,
            num="4.2.0.4.59",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractSeconds",
            level=5,
            num="4.2.0.4.60",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.subtractQuarters",
            level=5,
            num="4.2.0.4.61",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.dateDiff",
            level=5,
            num="4.2.0.4.62",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.timeSlots",
            level=5,
            num="4.2.0.4.63",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.DatesAndTimesFunctions.formatDateTime",
            level=5,
            num="4.2.0.4.64",
        ),
        Heading(name="Type Conversion Functions", level=3, num="4.2.1"),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toInt(8|16|32|64|128|256)",
            level=5,
            num="4.2.1.4.1",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUInt(8|16|32|64|256)",
            level=5,
            num="4.2.1.4.2",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toFloat(32|64)",
            level=5,
            num="4.2.1.4.3",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDate",
            level=5,
            num="4.2.1.4.4",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime",
            level=5,
            num="4.2.1.4.5",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64",
            level=5,
            num="4.2.1.4.6",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDateTime64.FromString.MissingTime",
            level=5,
            num="4.2.1.4.7",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toDecimal(32|64|128|256)",
            level=5,
            num="4.2.1.4.8",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toString",
            level=5,
            num="4.2.1.4.9",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.CAST(x,T)",
            level=5,
            num="4.2.1.4.10",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Milli",
            level=5,
            num="4.2.1.4.11",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Micro",
            level=5,
            num="4.2.1.4.12",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.toUnixTimestamp64Nano",
            level=5,
            num="4.2.1.4.13",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Milli",
            level=5,
            num="4.2.1.4.14",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Micro",
            level=5,
            num="4.2.1.4.15",
        ),
        Heading(
            name="RQ.SRS-010.DateTime64.ExtendedRange.TypeConversionFunctions.fromUnixTimestamp64Nano",
            level=5,
            num="4.2.1.4.16",
        ),
        Heading(name="References", level=1, num="5"),
    ),
    requirements=(
        RQ_SRS_010_DateTime64_ExtendedRange,
        RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start,
        RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start_BeforeEpochForTimeZone,
        RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End,
        RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End_AfterEpochForTimeZone,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions,
        RQ_SRS_010_DateTime64_ExtendedRange_TimeZones,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime,
        RQ_SRS_010_DateTime64_ExtendedRange_Comparison,
        RQ_SRS_010_DateTime64_ExtendedRange_SpecificTimestamps,
        RQ_SRS_010_DateTime64_ExtendedRange_Start,
        RQ_SRS_010_DateTime64_ExtendedRange_End,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidDate,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidTime,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_TimeZoneSwitch,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime_Disappeared,
        RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_LeapSeconds,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTimeZone,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYear,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toQuarter,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonth,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfYear,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfMonth,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfWeek,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toHour,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMinute,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toSecond,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toUnixTimestamp,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfYear,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfISOYear,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfQuarter,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMonth,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonday,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfWeek,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfDay,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfHour,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMinute,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfSecond,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFiveMinute,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfTenMinutes,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFifteenMinutes,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfInterval,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTime,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeYearNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeQuarterNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMonthNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeWeekNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeDayNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeHourNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMinuteNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeSecondNum,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOYear,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOWeek,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toWeek,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYearWeek,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_now,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_today,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_yesterday,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlot,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMM,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDD,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDDhhmmss,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addYears,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMonths,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addWeeks,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addDays,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addHours,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMinutes,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addSeconds,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addQuarters,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractYears,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMonths,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractWeeks,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractDays,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractHours,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMinutes,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractSeconds,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractQuarters,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_dateDiff,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlots,
        RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_formatDateTime,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toInt_8_16_32_64_128_256_,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUInt_8_16_32_64_256_,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toFloat_32_64_,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDate,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64_FromString_MissingTime,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDecimal_32_64_128_256_,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toString,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Milli,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Micro,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Nano,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Milli,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Micro,
        RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Nano,
    ),
    content="""
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
""",
)
