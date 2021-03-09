from testflows.core import *
import datetime

from datetime64_extended_range.requirements.requirements import *
from datetime64_extended_range.tests.common import *


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidDate("1.0")
)
def invalid_date(self):
    """Check how non-existent date is treated.
    For example, check 31st day in month that only has 30 days.
    """
    date_range = [1700, 1980, 2300]

    if self.context.stress:
        date_range = range(1698, 2378)

    with Step("I check 31st day of a 30-day month"):
        for year in date_range:
            for month in (4, 6, 9, 11):
                datetime = f"{year}-{str(month).zfill(2)}-31 12:23:34"
                expected = f"{year}-{str(month + 1).zfill(2)}-01 12:23:34"

                with Example(f"{datetime}", description=f"expected {expected}", flags=TE):
                    select_check_datetime(datetime=datetime, expected=expected)


@TestOutline(Suite)
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_InvalidTime("1.0")
)
@Examples(
    "datetime expected timezone", [
        ('2002-04-07 02:30:00', '2002-04-07 01:30:00', 'America/New_York'),
        ('2020-03-29 02:30:00', '2020-03-29 01:30:00', 'Europe/Zurich'),
        ('2017-03-26 02:30:00', '2017-03-26 01:30:00', 'Europe/Berlin')
    ])
def invalid_time(self, datetime, expected, timezone='UTC'):
    """proper handling of invalid time for a timezone
    when using DateTime64 extended range data type, for example,
    2:30am on 7th April 2002 never happened at all in the US/Eastern timezone,
    """
    with When(f"I check non-existent datetime {datetime}"):
        select_check_datetime(datetime=datetime, expected=expected, timezone=timezone)


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime("1.0"),
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime_Disappeared("1.0")
)
@Examples(
    "tz time_dates", [
        ('America/Denver', {'02:30:00': ('2018-03-11', '2020-03-08', '1980-04-27', '1942-02-09')}),
        ('Europe/Zurich', {'02:30:00': ('2016-03-27', '2020-03-29', '1981-03-29'), '01:30:00': ('1942-05-04', )})
])
def dst_disappeared(self, tz, time_dates):
    """Proper handling of switching DST, when an hour is being skipped.
    Testing in 2 steps: first, try to make a DateTime64 with skipped time value.
    Second, adding interval so that result is in the skipped time.
    """
    for time, dates in time_dates.items():
        for date in dates:
            with Given(f"forming a datetime"):
                dt_str = f"{date} {time}"
                dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            with Step("Assignment test"):
                with When("computing expected result"):
                    dt -= datetime.timedelta(hours=1)
                    expected = dt.strftime("%Y-%m-%d %H:%M:%S")
                with Then(f"I check skipped hour"):
                    select_check_datetime(datetime=dt_str, expected=expected, timezone=tz)
            with Step("Addition test"):
                with When("computing expected result"):
                    dt += datetime.timedelta(hours=2)
                    expected = dt.strftime("%Y-%m-%d %H:%M:%S")
                with Then(f"I check skipped hour"):
                    query = f"SELECT addHours(toDateTime64('{dt_str}', 0, '{tz}'), 1)"
                    exec_query(request=query, expected=f"{expected}")


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_LeapSeconds("1.0")
)
@Examples(
    "datet years", [
        ("06-30 23:59:55", [1972, 1981, 1982, 1983, 1985, 1992, 1993, 1994, 1997, 2012, 2015]),
        ("12-31 23:59:55", [1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1987, 1989, 1990, 1995, 1998, 2005, 2008, 2016])
])
def leap_seconds(self, datet, years):
    """Test proper handling of leap seconds. Read more: https://de.wikipedia.org/wiki/Schaltsekunde
    Being checked by selecting a timestamp prior to leap second and adding seconds so that the result is after it.
    """
    for year in years:
        with Example(f"{datet}, {year}"):
            with By("forming an expected result using python"):
                dt_str = f"{year}-{datet}"
                dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                dt += datetime.timedelta(seconds=9)
                expected = dt.strftime("%Y-%m-%d %H:%M:%S")
            with And(f"forming a query"):
                query = f"SELECT addSeconds(toDateTime64('{dt_str}', 0, 'UTC'), 10)"
            with Then("executing query"):
                exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_DaylightSavingTime("1.0"),
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime_TimeZoneSwitch("1.0")
)
def dst_time_zone_switch(self):
    """Check how ClickHouse supports handling of invalid time when using DateTime64 extended range data type
    when the invalid time is caused when countries switch timezone definitions with no daylight savings time switch.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)
    utc = pytz.timezone("UTC")

    for timezone in timezones:
        if timezone == 'UTC':
            continue
        with Example(f"{timezone}"):
            tz = pytz.timezone(timezone)
            transition_times = tz._utc_transition_times[1:]
            transition_info = tz._transition_info[1:]

            for i in range(len(transition_times)-1, 0, -1):
                if transition_times[i] > datetime.datetime.now():
                    continue
                with Step(f"{transition_times[i]}"):
                    with By("localize python datetime"):
                        dt = transition_times[i]
                        dt0 = dt - datetime.timedelta(hours=4)
                        dt0 = utc.localize(dt0).astimezone(tz).replace(tzinfo=None)
                    with And("compute expected result using Pytz"):
                        seconds_shift = transition_info[i][0] - transition_info[i-1][0]
                        dt1 = dt0 + datetime.timedelta(hours=8) + seconds_shift
                        dt0_str = dt0.strftime("%Y-%m-%d %H:%M:%S")
                        dt1_str = dt1.strftime("%Y-%m-%d %H:%M:%S")
                    with And("forming a ClickHouse query"):
                        query = f"SELECT addHours(toDateTime64('{dt0_str}', 0, '{timezone}'), 8)"
                    with Then("executing the query"):
                        exec_query(request=query, expected=f"{dt1_str}")




@TestFeature
@Name("non existent time")
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NonExistentTime("1.0")
)
def feature(self, node="clickhouse1"):
    """Check how ClickHouse treats non-existent time in DateTime64 data type.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)
