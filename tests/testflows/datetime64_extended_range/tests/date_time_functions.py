import time
import pytz
import itertools
from testflows.core import *
from dateutil.tz import tzlocal
import dateutil.relativedelta as rd
from datetime import datetime, timedelta

from datetime64_extended_range.requirements.requirements import *
from datetime64_extended_range.common import *
from datetime64_extended_range.tests.common import *


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTimeZone("1.0")
)
def to_time_zone(self):
    """Check the toTimeZone() function with DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given("I select datetimes in a year"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz1, tz2 in itertools.product(timezones, timezones):
                with Example(f"{dt} {tz1} -> {tz2}"):
                    with By("Computing expected output using pytz"):
                        dt_local = pytz.timezone(tz1).localize(dt)
                        dt_transformed = dt_local.astimezone(pytz.timezone(tz2))
                        tz2_expected = dt_transformed.strftime("%Y-%m-%d %H:%M:%S")
                    with And("Forming a toTimeZone ClickHouse query"):
                        tz1_query = dt_local.strftime("%Y-%m-%d %H:%M:%S")
                        query = f"SELECT toTimeZone(toDateTime64('{tz1_query}', 0, '{tz1}'), '{tz2}')"
                    with Then(f"I execute query", flags=TE):
                        exec_query(request=query, expected=f"{tz2_expected}")


@TestOutline
def to_date_part(self, py_func, ch_func):
    """Check the toYear/toMonth/toQuarter functions with DateTime64 extended range.
    """
    stress = self.context.stress

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
            timezones = timezones_range(stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1}, {tz2}"):
                        with Given("I compute expected output using pytz"):
                            with By(f"localizing {dt} using {tz1} timezone"):
                                time_tz1 = pytz.timezone(tz1).localize(dt)
                            with And(f"converting {tz1} local datetime {dt} to {tz2} timezone"):
                                time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            with And(f"calling the '{py_func}' method of the datetime object to get expected result"):
                                result = eval(f"time_tz2.{py_func}")
                            expected = f"{result}"
                        with And(f"Forming a {ch_func} ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {ch_func}(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then(f"I execute query that uses '{ch_func}' function"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYear("1.0"),
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeYearNum("1.0")
)
def to_year(self):
    """Check the toYear() and toRelativeYearNum() [which is just an alias for toYear]
    function with DateTime64 extended range.
    """
    to_date_part(py_func="year", ch_func="toYear")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonth("1.0")
)
def to_month(self):
    """Check the toMonth() function with DateTime64 extended range.
    """
    to_date_part(py_func="month", ch_func="toMonth")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toQuarter("1.0")
)
def to_quarter(self):
    """Check the toQuarter() function with DateTime64 extended range
    by comparing the output of the toQuarter() function with
    the value of the 'month' attribute of the datetime object
    divided by 3 (because each quarter has 3 month) plus 1
    (because we starting the count from 1).
    """
    to_date_part(py_func="month//3 + 1", ch_func="toQuarter")


@TestOutline
def to_day_of(self, py_func, ch_func):
    """Check the toDayOf....() functions with DateTime64 extended range.
    """
    stress = self.context.stress

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
            timezones = timezones_range(stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("Computing expected result using pytz"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            result = eval(f"time_tz2.timetuple().{py_func}")
                            expected = f"{result}"
                        with And(f"Forming a {ch_func} ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {ch_func}(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then(f"I execute {ch_func} query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfYear("1.0")
)
def to_day_of_year(self):
    """Check toDayOfYear() function with DateTime64 extended range date time.
    """
    to_day_of(py_func="tm_yday", ch_func="toDayOfYear")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfMonth("1.0")
)
def to_day_of_month(self):
    """Check toDayOfMonth() function with DateTime64 extended range date time.
    """
    to_day_of(py_func="tm_mday", ch_func="toDayOfMonth")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toDayOfWeek("1.0")
)
def to_day_of_week(self):
    """Check toDayOfWeek() function with DateTime64 extended range date time.
    """
    to_day_of(py_func="tm_wday", ch_func="toDayOfWeek")


@TestOutline
def to_time_part(self, py_func, ch_func):
    """Check the functions like toHour/toMinute/toSecond with DateTime64 extended range.
    """
    stress = self.context.stress

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
            timezones = timezones_range(stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("computing expected result using pytz"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            result = eval(f"time_tz2.{py_func}")
                            expected = f"{result}"
                        with And("forming a ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {ch_func}(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toHour("1.0")
)
def to_hour(self):
    """Check toHour() function with DateTime64 extended range date time.
    """
    to_time_part(py_func="hour", ch_func="toHour")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMinute("1.0")
)
def to_minute(self):
    """Check toMinute() function with DateTime64 extended range date time.
    """
    to_time_part(py_func="minute", ch_func="toMinute")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toSecond("1.0")
)
def to_second(self):
    """Check toSecond() function with DateTime64 extended range date time.
    """
    to_time_part(py_func="second", ch_func="toSecond")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toUnixTimestamp("1.0")
)
def to_unix_timestamp(self):
    """Check the toUnixTimestamp() function with DateTime64 extended range
    """
    stress = self.context.stress

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
            timezones = timezones_range(stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result using python"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            expected = f"{int(time.mktime(datetime.datetime.now().timetuple()))}"
                        with And("forming a ClickHouse query"):
                            query = f"SELECT toUnixTimestamp(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=expected)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfYear("1.0")
)
def to_start_of_year(self):
    """Check the functions toStartOfYear with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("computing expected time using python"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            expected = f"{time_tz2.year}-01-01"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfYear(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


def iso_year_start(dt):
    """Helper to find the beginning of iso year."""
    dt_s = datetime.datetime(dt.year-1, 12, 23, 0, 0, 0)
    while dt_s.isocalendar()[0] != dt.year:
        dt_s += datetime.timedelta(days=1)
    return dt_s


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfISOYear("1.0")
)
def to_start_of_iso_year(self):
    """Check the functions toStartOfISOYear with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("Computing expected result using Python"):
                            expected = iso_year_start(dt)
                        with And("Forming a toStartOfISOYear ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfISOYear(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute toStartOfISOYear query"):
                            exec_query(request=query, expected=f"{expected.strftime('%Y-%m-%d')}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfQuarter("1.0")
)
def to_start_of_quarter(self):
    """Check the functions toStartOfQuarter with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("computing expected result with python"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            expected = f"{year}-{str(time_tz2.month//3 * 3 + 1).zfill(2)}-01"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfQuarter(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMonth("1.0")
)
def to_start_of_month(self):
    """Check the functions toStartOfMonth with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("computing expected result with python"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            expected = f"{time_tz2.strftime('%Y-%m')}-01"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfQuarter(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toMonday("1.0")
)
def to_monday(self):
    """Check the functions toMonday with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    with Example(f"{dt} {tz1} -> {tz2}"):
                        with By("computing expected result with python"):
                            time_tz1 = pytz.timezone(tz1).localize(dt)
                            time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                            expected_date = time_tz2 + datetime.timedelta(days=(-dt.weekday() if dt.weekday() <= 3 else 7 - dt.weekday()))
                            expected = f"{expected_date.strftime('%Y-%m-%d')}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toMonday(toDateTime64('{dt_str}', 0, '{tz1}'), '{tz2}')"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfWeek("1.0")
)
def to_start_of_week(self):
    """Check the functions toStartOfWeek with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz1, tz2 in itertools.product(timezones, timezones):
                    for mode in (0, 1):   # mode - week beginning, either 0 (Sunday) or 1 (Monday)
                        with Example(f"{dt} {tz1} -> {tz2}"):
                            with By("computing expected result with python"):
                                time_tz1 = pytz.timezone(tz1).localize(dt)
                                time_tz2 = time_tz1.astimezone(pytz.timezone(tz2))
                                expected_date = time_tz2 + datetime.timedelta(
                                        days=(mode - dt.weekday() if dt.weekday() <= (3+mode) else (mode + 7) - dt.weekday()))
                                expected = f"{expected_date.strftime('%Y-%m-%d')}"
                            with And("forming ClickHouse query"):
                                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                                query = f"SELECT toStartOfWeek(toDateTime64('{dt_str}', 0, '{tz1}'), {mode}, '{tz2}')"
                            with Then("I execute query"):
                                exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfDay("1.0")
)
def to_start_of_day(self):
    """Check the functions toStartOfDay with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result with python"):
                            expected = f"{dt.strftime('%Y-%m-%d')} 00:00:00"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfDay(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfHour("1.0")
)
def to_start_of_hour(self):
    """Check the functions toStartOfHour with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result with python"):
                            expected = f"{dt.strftime('%Y-%m-%d %H')}:00:00"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfHour(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfMinute("1.0")
)
def to_start_of_minute(self):
    """Check the functions toStartOfMinute with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result with python"):
                            expected = f"{dt.strftime('%Y-%m-%d %H:%M')}:00"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toStartOfMinute(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfSecond("1.0")
)
def to_start_of_second(self):
    """Check the functions toStartOfSecond with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result with python"):
                            expected = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.000000"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                            query = f"SELECT toStartOfSecond(toDateTime64('{dt_str}', 6, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestOutline
def to_start_of_minutes_interval(self, interval, func):
    """Check the functions like toStartOf....Minute with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("Computing expected result using python"):
                            mins = dt.minute // interval * interval
                            expected = f"{dt.strftime('%Y-%m-%d %H:')}{str(mins).zfill(2)}:00"
                        with And(f"Forming a {func} query to ClickHouse"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {func}(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then(f"I execute {func} query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFiveMinute("1.0")
)
def to_start_of_five_minute(self):
    """Check the toStartOfFiveMinute with DateTime64 extended range."""
    to_start_of_minutes_interval(interval=5, func="toStartOfFiveMinute")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfTenMinutes("1.0")
)
def to_start_of_ten_minutes(self):
    """Check the toStartOfTenMinutes with DateTime64 extended range."""
    to_start_of_minutes_interval(interval=10, func="toStartOfTenMinutes")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfFifteenMinutes("1.0")
)
def to_start_of_fifteen_minutes(self):
    """Check the toStartOfFifteenMinutes with DateTime64 extended range."""
    to_start_of_minutes_interval(interval=15, func="toStartOfFifteenMinutes")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlot("1.0")
)
def time_slot(self):
    """Check the timeSlot with DateTime64 extended range."""
    to_start_of_minutes_interval(interval=30, func="timeSlot")


def to_start_of_interval_helper(dt: datetime.datetime, interval_type, interval_value):
    """A helper to switch through all possible intervals. Computes the interval beginning depending on
    interval_type and interval_value, returns string expected to be returned by ClickHouse.
    :param dt: datetime to be checked, datetime.datetime
    :param interval_type: interval type selector, String
    :param interval_value: interval size, int
    """
    intervals_in_seconds = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 68400, "WEEK": 604800}
    zero_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0)
    delta = dt - zero_datetime

    if interval_type in intervals_in_seconds.keys():
        divisor = interval_value * intervals_in_seconds[interval_type]
        retval = (zero_datetime + datetime.timedelta(seconds=(delta.seconds // divisor * divisor)))
        if interval_type == "WEEK":
            return retval.strftime("%Y-%m-%d")
        return retval.strftime("%Y-%m-%d %H:%M:%S")

    elif interval_type == "MONTH":
        diff = (dt.year - zero_datetime.year) * 12 + (dt.month - zero_datetime.month)
        result_diff = diff // interval_value * interval_value
        return (zero_datetime + rd.relativedelta(months=result_diff)).strftime("%Y-%m-%d")

    elif interval_type == "QUARTER":
        diff = (dt.year - zero_datetime.year) * 4 + (dt.month // 4 - zero_datetime.month // 4)
        result_diff = diff // interval_value * interval_value
        return (zero_datetime + rd.relativedelta(months=result_diff*4)).strftime("%Y-%m-%d")

    elif interval_type == "YEAR":
        result_diff = (dt.year - zero_datetime.year) // interval_value * interval_value
        return (zero_datetime + rd.relativedelta(years=result_diff)).strftime("%Y-%m-%d")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toStartOfInterval("1.0")
)
def to_start_of_interval(self):
    """Check the toStartOfInterval with DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    intervals_testing_ranges = {"SECOND": range(1, 15), "MINUTE": range(1, 15), "HOUR": range(1, 10), "DAY": (1, 5, 10),
                                "WEEK": range(1, 5), "MONTH": range(1, 6), "QUARTER": range(1, 4), "YEAR": range(1, 5)}

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    for interval in intervals_testing_ranges.keys():
                        for value in intervals_testing_ranges[interval]:
                            with Example(f"{dt} {tz} {interval}: {value}"):
                                with By("Computing expected result using python"):
                                    expected = to_start_of_interval_helper(dt, interval, value)
                                with And(f"Forming a toStartOfInterval() query to ClickHouse"):
                                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                                    query = f"SELECT toStartOfInterval(toDateTime64('{dt_str}', 0, '{tz}'), INTERVAL {value} {interval})"
                                with Then(f"I execute toStartOfInterval() query"):
                                    exec_query(request=query, expected=f"{expected}")


@TestOutline
def to_iso(self, func, isocalendar_pos):
    """Check the toISOYear/Week functions with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computimg expected result uning python"):
                            expected = f"{dt.isocalendar()[isocalendar_pos]}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {func}(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOYear("1.0")
)
def to_iso_year(self):
    """Check the toISOYear function with DateTime64 extended range."""
    to_iso(func="toISOYear", isocalendar_pos=0)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toISOWeek("1.0")
)
def to_iso_week(self):
    """Check the toISOWeek function with DateTime64 extended range."""
    to_iso(func="toISOWeek", isocalendar_pos=1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toTime("1.0")
)
def to_time(self):
    """Check the toTime function with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result using python"):
                            expected = f"1970-01-02 {dt.strftime('%H:%M:%S')}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toTime(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeQuarterNum("1.0")
)
def to_relative_quarter_num(self):
    """Check the toRelativeQuarterNum function with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result using python"):
                            expected = f"{(year-1970)*4 + (dt.month - 1) // 3}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toRelativeQuarterNum(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeWeekNum("1.0")
)
def to_relative_week_num(self):
    """Check the toRelativeWeekNum function with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result using python"):
                            week_num = ((dt + datetime.timedelta(days=8) - datetime.timedelta(days=dt.weekday())) - datetime.datetime(1970, 1, 1, 0, 0, 0)).days // 7
                            expected = f"{week_num}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toRelativeWeekNum(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMonthNum("1.0")
)
def to_relative_month_num(self):
    """Check the toRelativeMonthNum function with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result using python"):
                            month_num = (year - 1970) * 12 + dt.month
                            expected = f"{month_num}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toRelativeMonthNum(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeDayNum("1.0")
)
def to_relative_day_num(self):
    """Check the toRelativeDayNum function with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("Computing the expected result using python"):
                            day_num = (dt - datetime.datetime(1970, 1, 1, 0, 0, 0)).days
                            expected = f"{day_num}"
                        with And(f"Forming a toRelativeDayNum query to ClickHouse"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toRelativeDayNum(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with When("I execute toRelativeDayNum query"):
                            exec_query(request=query, expected=f"{expected}")


@TestOutline
def to_relative_time(self, divisor, func):
    """Check the toRelative[Hour/Minute/Second]Num functions
    with DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress, microseconds=True)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("Computing the expected result using python"):
                            result = (dt - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds() // divisor
                            expected = f"{result}"
                        with And(f"Forming a {func} query to ClickHouse"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT {func}(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then(f"I execute {func} query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeHourNum("1.0")
)
def to_relative_hour_num(self):
    """Check the toRelativeHourNum function
    with DateTime64 extended range.
    """
    to_relative_time(func="toRelativeHourNum", divisor=3600)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeMinuteNum("1.0")
)
def to_relative_minute_num(self):
    """Check the toRelativeMinuteNum function
    with DateTime64 extended range.
    """
    to_relative_time(func="toRelativeMinuteNum", divisor=60)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toRelativeSecondNum("1.0")
)
def to_relative_second_num(self):
    """Check the toRelativeSecondNum function
    with DateTime64 extended range.
    """
    to_relative_time(func="toRelativeSecondNum", divisor=1)


def to_week_compute_expected(dt: datetime.datetime, mode: int, ret_year=False):
    """Helper to get the expected value for testing toWeek().
    Due to necessity to manually check 10 workmodes, the subroutine was removed from to_week()
    """
    year = dt.year

    ex = datetime.datetime(year, 1, 1)
    j1_weekday = ex.weekday()

    while ex.weekday() != 0:
        ex += datetime.timedelta(days=1)
    first_monday = ex.day-1

    ex = datetime.datetime(year, 1, 1)
    while ex.weekday() != 6:
        ex += datetime.timedelta(days=1)
    first_sunday = ex.day-1

    if mode == 0:
        # First day of week: Sunday, Week 1 is the first week with Sunday, range 0-53
        expected = (dt - datetime.datetime(year, 1, 1) - datetime.timedelta(days=first_sunday)).days // 7 + 1

    elif mode == 1:
        # First day of week: Monday, Week 1 is the first week containing 4 or more days, range 0-53
        if j1_weekday <= 3:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=7+j1_weekday)).days // 7
        else:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=j1_weekday)).days // 7

    elif mode == 2:
        # First day of week: Sunday, Week 1 is the first week with Sunday, range 1-53
        expected = (dt - datetime.datetime(year, 1, 1) - datetime.timedelta(days=first_sunday)).days // 7 + 1
        if expected == 0:
            return to_week_compute_expected(datetime.datetime(dt.year-1, 12, 31), 2)

    elif mode == 3:
        # First day of week: Monday, Week 1 is the first week containing 4 or more days, range 1-53
        if j1_weekday <= 3:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=7+j1_weekday)).days // 7
        else:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=j1_weekday)).days // 7
        if expected == 0:
            return to_week_compute_expected(datetime.datetime(dt.year-1, 12, 31), 3)

    elif mode == 4:
        # First day of week: Sunday, Week 1 is the first week containing 4 or more days, range 0-53
        if j1_weekday <= 3:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=8+j1_weekday)).days // 7
        else:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=j1_weekday+1)).days // 7

    elif mode == 5:
        # First day of week: Monday, Week 1 is the first week with a Monday, range 0-53
        expected = (dt - datetime.datetime(year, 1, 1) - datetime.timedelta(days=first_monday)).days // 7 + 1

    elif mode == 6:
        # First day of week: Sunday, Week 1 is the first week containing 4 or more days, range 1-53
        if j1_weekday <= 3:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=8+j1_weekday)).days // 7
        else:
            expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=j1_weekday+1)).days // 7
        if expected == 0:
            return to_week_compute_expected(datetime.datetime(dt.year-1, 12, 31), 6)

    elif mode == 7:
        # First day of week: Monday, Week 1 is the first week with a Monday, range 1-53
        expected = (dt - datetime.datetime(year, 1, 1) - datetime.timedelta(days=first_monday)).days // 7 + 1
        if expected == 0:
            return to_week_compute_expected(datetime.datetime(dt.year-1, 12, 31), 7)

    elif mode == 8:
        # First day of week: Sunday, Week 1 is the week containing January 1, range 1-53
        expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=(j1_weekday+1)%7)).days // 7 + 1

    elif mode == 9:
        # First day of week: Monday, Week 1 is the week containing January 1, range 1-53
        expected = (dt - datetime.datetime(year, 1, 1) + datetime.timedelta(days=j1_weekday%7)).days // 7 + 1

    return f"{dt.year}{str(expected).zfill(2)}" if ret_year else f"{expected}"


@TestOutline
def to_week_year_week(self, clh_func, ret_year):
    """Check the toWeek/toYearWeek function with DateTime64 extended range.
    For detailed info on work modes and description, see
    https://clickhouse.tech/docs/en/sql-reference/functions/date-time-functions/#toweekdatemode
    :param self: current test
    :param clh_func: ClickHouse function to be called, string
    :param ret_year: toWeek/toYearWeek selector, Boolean
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    for mode in range(0, 10):
                        with Example(f"{dt} {tz}"):
                            with By("Computing expected output using python"):
                                expected = to_week_compute_expected(dt=dt, mode=mode, ret_year=ret_year)
                            with And(f"Forming a {clh_func} query"):
                                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                                query = f"SELECT {clh_func}(toDateTime64('{dt_str}', 0, '{tz}'), {mode})"
                            with When(f"I execute the {clh_func} query"):
                                exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toWeek("1.0")
)
def to_week(self):
    """Check the toWeek function with DateTime64 extended range."""
    to_week_year_week(clh_func="toWeek", ret_year=False)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYearWeek("1.0")
)
def to_year_week(self):
    """Check the toYearWeek function with DateTime64 extended range."""
    to_week_year_week(clh_func="toYearWeek", ret_year=True)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMM("1.0")
)
def to_yyyymm(self):
    """Check the toYYYYMM() function with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result in python"):
                            expected = f"{dt.strftime('%Y%m')}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toYYYYMM(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDD("1.0")
)
def to_yyyymmdd(self):
    """Check the toYYYYMMDD() function with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result in python"):
                            expected = f"{dt.strftime('%Y%m%d')}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toYYYYMMDD(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query", description=f"expected {expected}", flags=TE):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_toYYYYMMDDhhmmss("1.0")
)
def to_yyyymmddhhmmss(self):
    """Check the toYYYYMMDDhhmmss() function with
    DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    with Example(f"{dt} {tz}"):
                        with By("computing expected result in python"):
                            expected = f"{dt.strftime('%Y%m%d%H%M%S')}"
                        with And("forming ClickHouse query"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"SELECT toYYYYMMDDhhmmss(toDateTime64('{dt_str}', 0, '{tz}'))"
                        with Then("I execute query"):
                            exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_now("1.0")
)
def now(self):
    """Check the now() conversion to DateTime64 extended range.
    In this test, we cannot assure that pytz now() and ClickHouse now() will be executed at the same time, so we need
    a 30-second error tolerance to test this functionality
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for tz in timezones:
        with Given("I record current time and localize it"):
            dt = datetime.datetime.now(tzlocal())
            dt = dt.astimezone(pytz.timezone(tz))

        with Example(f"{dt} {tz}"):
            with When("I execute query and format its result to string"):
                r = self.context.node.query(f"SELECT toDateTime64(now(), 0, '{tz}')")
                query_result = r.output
                received_dt = datetime.datetime.strptime(query_result, '%Y-%m-%d %H:%M:%S')

            with Then("I compute the difference between ClickHouse query result and pytz result"):
                dt = dt.replace(tzinfo=None)
                if dt < received_dt:
                    diff = (received_dt - dt).total_seconds()
                else:
                    diff = (dt - received_dt).total_seconds()

            with Finally(f"I expect {diff} < 30 seconds"):
                assert diff < 30, error()


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_today("1.0")
)
def today(self):
    """Check the today() conversion to DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for tz in timezones:
        with Given("I record current time and localize it"):
            dt = datetime.datetime.now(tzlocal())
            dt = dt.astimezone(pytz.timezone(tz))

        with Example(f"{dt} {tz}"):
            with When("I execute query and format its result to string"):
                r = self.context.node.query(f"SELECT toDateTime64(today(), 0, '{tz}')")
                query_result = r.output
                received_dt = datetime.datetime.strptime(query_result, '%Y-%m-%d %H:%M:%S')

            with Then("I compute the difference between ClickHouse query result and pytz result"):
                dt = dt.replace(tzinfo=None)
                if dt < received_dt:
                    diff = (received_dt - dt).total_seconds()
                else:
                    diff = (dt - received_dt).total_seconds()

            with Finally(f"I expect {diff} < 24 hours"):
                assert diff < 86400, error()


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_yesterday("1.0")
)
def yesterday(self):
    """Check the yesterday() conversion to DateTime64 extended range.
    """

    stress = self.context.stress
    timezones = timezones_range(stress)

    for tz in timezones:
        with Given("I record current time and localize it"):
            dt = datetime.datetime.now(tzlocal())
            dt = dt.astimezone(pytz.timezone(tz))

        with Example(f"{dt} {tz}"):
            with When("I execute query and format its result to string"):
                r = self.context.node.query(f"SELECT toDateTime64(yesterday(), 0, '{tz}')")
                query_result = r.output
                received_dt = datetime.datetime.strptime(query_result, '%Y-%m-%d %H:%M:%S')

            with Then("I compute the difference between ClickHouse query result and pytz result"):
                dt = dt.replace(tzinfo=None)
                dt -= datetime.timedelta(days=1)
                if dt < received_dt:
                    diff = (received_dt - dt).total_seconds()
                else:
                    diff = (dt - received_dt).total_seconds()

            with Finally(f"I expect {diff} < 48 hours (172800 seconds)"):
                assert diff < 172800, error()


@TestOutline
def add_subtract_functions(self, clh_func, py_key, test_range, years_padding=(1, 1), modifier=1, mult=1):
    """Check the addYears/addMonths/addWeeks/addDays/addHours/addMinutes/addSeconds with DateTime64 extended range.
    Calculating expected result using eval() to avoid writing 9000+ comparisons and just parse string as object field name.
    :param self: self
    :param clh_func: ClickHouse function to be called, string
    :param py_key: relativedelta parameter used to modify datetime, string
    :param test_range: range of increments to be used, any iterable ints structure
    :param years_padding: a number of years to be padded from the border, tuple[int, int]
    :param modifier: a modifier to calculate [add/subtract]Quarters(), [add/subtract]Weeks()
    :param mult: +/- selector
    """
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress=stress, padding=years_padding):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I check each of the datetimes"):
            for dt in datetimes:
                for tz in timezones:
                    for incr in test_range:
                        with Example(f"{dt} {tz}"):
                            with By("converting datetime to string"):
                                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            with And("computing the expected result using pytz"):
                                dt = eval(f"dt + rd.relativedelta({py_key}={mult*incr*modifier})")
                                expected = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}"
                            with And("making a query string for ClickHouse"):
                                query = f"SELECT {clh_func}(toDateTime64('{dt_str}', 0, '{tz}'), {incr})"
                            with Then("I execute query"):
                                exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addYears("1.0")
)
def add_years(self):
    """Check the addYears function with DateTime64 extended range."""
    add_subtract_functions(clh_func="addYears", py_key="years", test_range=(0, 1), years_padding=(0, 1))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractYears("1.0")
)
def subtract_years(self):
    """Check the subtractYears function with DateTime64 extended range."""
    add_subtract_functions(clh_func="subtractYears", py_key="years", test_range=(0, 1), years_padding=(1, 0), mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addQuarters("1.0")
)
def add_quarters(self):
    """Check the addQuarters function with DateTime64 extended range."""
    add_subtract_functions(clh_func="addQuarters", py_key="months", test_range=range(1, 5), years_padding=(0, 1), modifier=3)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractQuarters("1.0")
)
def subtract_quarters(self):
    """Check the subtractQuarters function with DateTime64 extended range."""
    add_subtract_functions(clh_func="subtractQuarters", py_key="months", test_range=range(1, 5), years_padding=(1, 0), modifier=3, mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMonths("1.0")
)
def add_months(self):
    """Check the addMonths function with DateTime64 extended range."""
    add_subtract_functions(clh_func="addMonths", py_key="months", test_range=range(1, 13), years_padding=(0, 1))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMonths("1.0")
)
def subtract_months(self):
    """Check the subtractMonths function with DateTime64 extended range."""
    add_subtract_functions(clh_func="subtractMonths", py_key="months", test_range=range(1, 13), years_padding=(1, 0), mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addWeeks("1.0")
)
def add_weeks(self):
    """Check the addWeeks function with DateTime64 extended range."""
    add_subtract_functions(clh_func="addWeeks", py_key="days", test_range=range(6), years_padding=(0, 1), modifier=7)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractWeeks("1.0")
)
def subtract_weeks(self):
    """Check the subtractWeeks function with DateTime64 extended range."""
    add_subtract_functions(clh_func="subtractWeeks", py_key="days", test_range=range(6), years_padding=(1, 0), modifier=7, mult=-1)



@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addDays("1.0")
)
def add_days(self):
    """Check the addDays function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="addDays", py_key="days", test_range=range(50))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractDays("1.0")
)
def subtract_days(self):
    """Check the subtractDays function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="subtractDays", py_key="days", test_range=range(50), mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addHours("1.0")
)
def add_hours(self):
    """Check the addHours function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="addHours", py_key="hours", test_range=range(25))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractHours("1.0")
)
def subtract_hours(self):
    """Check the subtractHours function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="subtractHours", py_key="hours", test_range=range(25), mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addMinutes("1.0")
)
def add_minutes(self):
    """Check the addMinutes function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="addMinutes", py_key="minutes", test_range=range(60))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractMinutes("1.0")
)
def subtract_minutes(self):
    """Check the subtractMinutes function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="subtractMinutes", py_key="minutes", test_range=range(60), mult=-1)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_addSeconds("1.0")
)
def add_seconds(self):
    """Check the addSeconds function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="addSeconds", py_key="seconds", test_range=range(60))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_subtractSeconds("1.0")
)
def subtract_seconds(self):
    """Check the subtractSeconds function work with DateTime64 extended range"""
    add_subtract_functions(clh_func="subtractSeconds", py_key="seconds", test_range=range(60), mult=-1)


def date_diff_helper(dt1, dt2: datetime.datetime, unit: str):
    """Helper for computing dateDiff expected result using Python.
    """
    delta = dt2 - dt1
    if unit == "second":
        return delta.total_seconds()
    elif unit == "minute":
        return delta.total_seconds() // 60
    elif unit == "hour":
        return delta.total_seconds() // 3600
    elif unit == "day":
        return delta.total_seconds() // 86400
    elif unit == "week":
        return delta.total_seconds() // 604800
    elif unit == "month":
        return (dt2.year - dt1.year) * 12 + (dt2.month - dt1.month)
    elif unit == "quarter":
        return ((dt2.year - dt1.year) * 12 + (dt2.month - dt1.month)) // 3
    elif unit == "year":
        return dt2.year - dt1.year


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_dateDiff("1.0")
)
def date_diff(self):
    """Check how dateDiff works with DateTime64 extended range.
    """
    stress = self.context.stress
    compare_units = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
    timezones = timezones_range(stress=stress)

    with Background("I select a set of datetimes to be compared"):
        datetimes = []
        for year in years_range(stress=stress):
            datetimes += list(select_dates_in_year(year=year, stress=stress))

    for dt_1, dt_2 in itertools.product(datetimes, datetimes):
        for tz in timezones:
            dt1_str = dt_1.strftime("%Y-%m-%d %H:%M:%S")
            dt2_str = dt_2.strftime("%Y-%m-%d %H:%M:%S")
            dt1 = pytz.timezone(tz).localize(dt_1)
            dt2 = pytz.timezone(tz).localize(dt_2)

            for unit in compare_units:
                with Example(f"{unit}: {dt1_str} {dt2_str}, {tz}"):
                    with When("I compute expected result with Pythons"):
                        expected = date_diff_helper(dt1=dt1, dt2=dt2, unit=unit)
                    with Then(f"I check dateDiff {dt1_str} {dt2_str} in {unit}"):
                        query = f"SELECT dateDiff('{unit}', toDateTime64('{dt1_str}', 0, '{tz}'), toDateTime64('{dt2_str}', 0, '{tz}'))"
                        exec_query(request=query, expected=expected)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_formatDateTime("1.0")
)
def format_date_time(self):
    """Test formatDateTime() when DateTime64 is out of normal range.
     This function formats DateTime according to a given Format string.
     """
    stress = self.context.stress
    timezones = timezones_range(stress)

    modes = ('C', 'd', 'D', 'e', 'F', 'G', 'g', 'H', 'I', 'j', 'm', 'M', 'n',
             'p', 'R', 'S', 't', 'T', 'u', 'V', 'w', 'y', 'Y', '%')

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        with When("I format each of the datetimes in every possible way"):
            for dt in datetimes:
                for tz in timezones:
                    for mode in modes:
                        with Example(f"{dt} {tz}, format '%{mode}"):
                            with By("converting datetime to string"):
                                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            with And("computing the expected result using python"):
                                expected = f"{dt.strftime(f'%{mode}')}"
                            with And("making a query string for ClickHouse"):
                                query = f"SELECT formatDateTime(toDateTime64('{dt_str}', 0, '{tz}'), '%{mode}')"
                            with Then("I execute formatDateTime() query"):
                                exec_query(request=query, expected=f"{expected}")


def time_slots_get_expected(dt: datetime.datetime, duration, size=1800):
    """Helper to compute expected array for timeSlots().
    """
    zero_time = datetime.datetime(1970, 1, 1, 0, 0, 0)

    result = [(zero_time + datetime.timedelta(seconds=((dt - zero_time).total_seconds() // size * size))).strftime("%Y-%m-%d %H:%M:%S")]

    s = 1
    while s <= duration:
        delta = dt - zero_time + datetime.timedelta(seconds=s)
        seconds_rounded = delta.total_seconds() // size * size
        r = zero_time + datetime.timedelta(seconds=seconds_rounded)
        r_str = r.strftime("%Y-%m-%d %H:%M:%S")
        if r_str not in result:
            result.append(r_str)
            s += size
        else:
            s += 1
    return result


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions_timeSlots("1.0")
)
def time_slots(self):
    """Check the timeSlots function with DateTime64 extended range.
    syntax: timeSlots(StartTime, Duration _seconds_ [, Size _seconds_])
    """
    stress = self.context.stress

    for year in years_range(stress=stress):
        with Given(f"I choose datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)
        with When("I check each of the datetimes"):
            for dt in datetimes:
                for duration in range(1, 100, 9):
                    for size in range(1, 50, 3):
                        with Example(f"{dt}, dur={duration}, size={size}"):
                            with By("getting an expected array using python"):
                                expected = time_slots_get_expected(dt=dt, duration=duration, size=size)
                            with And("forming a ClickHouse query"):
                                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                                query = f"SELECT timeSlots(toDateTime64('{dt_str}', 0, 'UTC'), toUInt32({duration}), {size})"
                            with Then("I execute query"):
                                try:
                                    assert eval(self.context.node.query(query).output) == expected, error()
                                except SyntaxError:
                                    assert False


@TestFeature
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_DatesAndTimesFunctions("1.0")
)
def date_time_funcs(self, node="clickhouse1"):
    """Check the basic operations with DateTime64
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)    