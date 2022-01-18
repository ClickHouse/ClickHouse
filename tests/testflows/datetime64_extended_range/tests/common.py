import pytz
import datetime

from testflows.core import *
from testflows.asserts import error
from contextlib import contextmanager
from datetime64_extended_range.common import *


def in_normal_range(dt: datetime.datetime):
    """Check if DateTime is in normal range
    """
    return dt <= datetime.datetime(2105, 12, 31, 23, 59, 59, 999999) and dt >= datetime.datetime(1970, 1, 1, 0, 0, 0)


def years_range(stress=False, padding=(0, 0)):
    """Returns a set of year values used for testing.
    """
    return range(1925+padding[0], 2283-padding[1]) if stress else (1927, 2000, 2281)


def timezones_range(stress=False):
    """Returns a set of timezone values used for testing.
    """
    if stress:
        return pytz.all_timezones
    else:
        return ['UTC', 'Asia/Novosibirsk', 'America/Denver']


@contextmanager
def create_table(timezone, node):
    try:
        node.query(f"CREATE TABLE dt(timestamp DateTime64(3, {timezone})) Engine = TinyLog")
        yield
    finally:
        node.query("DROP TABLE dt")


@TestOutline
def insert_check_datetime(self, datetime, expected, precision=0, timezone="UTC"):
    """Check how a particular datetime value works with different
    functions that accept DateTime64 data type.

    :param datetime: datetime string
    :param expected: expected result
    :param precision: time precision, default: 0
    :param timezone: timezone, default: UTC
    """
    with create_table(timezone, self.context.node):
        with When("I use toDateTime64"):
            r = self.context.node.query(f"SELECT toDateTime64('{datetime}', {precision}, '{timezone}')")

    with Then(f"I expect {expected}"):
        assert r.output == expected, error()


def datetime_generator(year, microseconds=False):
    """Helper generator
    """
    date = datetime.datetime(year, 1, 1, 0, 0, 0)
    if microseconds:
        date = datetime.datetime(year, 1, 1, 0, 0, 0, 123000)
    while not (date.month == 12 and date.day == 31):
        yield date
        date = date + datetime.timedelta(days=1, hours=1, minutes=1, seconds=1)


def select_dates_in_year(year, stress=False, microseconds=False):
    """Returns various datetimes in a year that are to be checked
    """
    if not stress:
        dates = [datetime.datetime(year, 1, 1, 0, 0, 0), datetime.datetime(year, 12, 31, 23, 59, 59)]
        if microseconds:
            dates = [datetime.datetime(year, 1, 1, 0, 0, 0, 123000), datetime.datetime(year, 12, 31, 23, 59, 59, 123000)]
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            dates.append(datetime.datetime(year, 2, 29, 11, 11, 11, 123000))
        return dates
    else:
        return datetime_generator(year)


@TestOutline
def select_check_datetime(self, datetime, expected, precision=0, timezone="UTC"):
    """Check how a particular datetime value works with different
    functions that accept DateTime64 data type.

    :param datetime: datetime string
    :param expected: expected result
    :param precision: time precision, default: 0
    :param timezone: timezone, default: UTC
    """
    with When("I use toDateTime64"):
        r = self.context.node.query(f"SELECT toDateTime64('{datetime}', {precision}, '{timezone}')")

    with Then(f"I expect {expected}"):
        assert r.output == expected, error()


@TestStep(When)
def exec_query(self, request, expected=None, exitcode=None):
    """Execute a query and check expected result.
    :param request: query string
    :param expected: result string
    :param exitcode: exitcode
    """
    r = self.context.node.query(request)

    if expected is not None:
        with Then(f"output should match the expected", description=f"{expected}"):
            assert r.output == expected, error()

    elif exitcode is not None:
        with Then(f"output exitcode should match expected", description=f"{exitcode}"):
            assert r.exitcode == exitcode, error()


@TestStep
def walk_datetime_in_incrementing_steps(self, date, hrs_range=(0, 24), step=1, timezone="UTC", precision=0):
    """Sweep time starting from some start date. The time is incremented
    in steps specified by the `step` parameter
    (default: 1 min).

    :param hrs_range: range in hours
    :param step: step in minutes
    """

    stress = self.context.stress
    secs = f"00{'.' * (precision > 0)}{'0' * precision}"

    tasks = []
    with Pool(2) as pool:
        try:
            with When(f"I loop through datetime range {hrs_range} starting from {date} in {step}min increments"):
                for hrs in range(*hrs_range) if stress else (hrs_range[0], hrs_range[1]-1):
                    for mins in range(0, 60, step) if stress else (0, 59):
                        datetime = f"{date} {str(hrs).zfill(2)}:{str(mins).zfill(2)}:{secs}"
                        expected = datetime

                        with When(f"time is {datetime}"):
                            run_scenario(pool, tasks, Test(name=f"{hrs}:{mins}:{secs}", test=select_check_datetime),
                                         kwargs=dict(datetime=datetime, precision=precision, timezone=timezone,
                                                     expected=expected))
        finally:
            join(tasks)


@TestStep
def walk_datetime_in_decrementing_steps(self, date, hrs_range=(23, 0), step=1, timezone="UTC", precision=0):
    """Sweep time starting from some start date. The time is decremented
    in steps specified by the `step` parameter
    (default: 1 min).

    :param date: string
    :param hrs_range: range in hours
    :param step: step in minutes
    :param timezone: String
    """
    stress = self.context.stress
    secs = f"00{'.' * (precision > 0)}{'0' * precision}"

    tasks = []
    with Pool(2) as pool:
        try:
            with When(f"I loop through datetime range {hrs_range} starting from {date} in {step}min decrements"):
                for hrs in range(*hrs_range, -1) if stress else (hrs_range[1], hrs_range[0]):
                    for mins in range(59, 0, -step) if stress else (59, 0):
                        datetime = f"{date} {str(hrs).zfill(2)}:{str(mins).zfill(2)}:{secs}"
                        expected = datetime

                        with When(f"time is {datetime}"):
                            run_scenario(pool, tasks, Test(name=f"{hrs}:{mins}:{secs}", test=select_check_datetime),
                                         kwargs=dict(datetime=datetime, precision=precision, timezone=timezone,
                                                     expected=expected))
        finally:
            join(tasks)
