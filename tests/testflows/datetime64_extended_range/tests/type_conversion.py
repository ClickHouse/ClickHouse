import time
import pytz
import decimal
import itertools
import numpy as np
from dateutil.tz import tzlocal
from datetime import datetime, timedelta
import dateutil.relativedelta as rd
from testflows.core import *

from datetime64_extended_range.requirements.requirements import *
from datetime64_extended_range.common import *
from datetime64_extended_range.tests.common import *


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toInt_8_16_32_64_128_256_(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_int_8_16_32_64_128_256(self, cast):
    """Check the toInt(8|16|32|64|128|256) functions with DateTime64 extended range"""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                for int_type in (8, 16, 32, 64, 128, 256):
                    with When(f"{dt} {tz}, int{int_type}"):
                        with By("converting datetime to string"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                        with And("computing the expected result using python"):
                            py_res = int(time.mktime(dt.timetuple()))
                            expected = f"{py_res}"
                            if not (int_type == 128 or int_type == 256):
                                """ALL dates fit into int128 and int256, so no need to check"""
                                np_res = exec(f"np.int{int_type}({py_res})")
                            else:
                                np_res = py_res
                        if np_res == py_res:
                            with Given(f"{py_res} fits int{int_type}"):
                                with When(
                                    f"making a query string for ClickHouse if py_res fits int{int_type}"
                                ):
                                    if cast:
                                        query = f"SELECT cast(toDateTime64('{dt_str}', 0, '{tz}'), 'Int{int_type}')"
                                    else:
                                        query = f"SELECT toInt{int_type}(toDateTime64('{dt_str}', 0, '{tz}'))"
                                with Then(f"I execute toInt{int_type}() query"):
                                    exec_query(request=query, expected=f"{expected}")


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUInt_8_16_32_64_256_(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_uint_8_16_32_64_256(self, cast):
    """Check the toUInt(8|16|32|64|256) functions with DateTime64 extended range"""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                for int_type in (8, 16, 32, 64, 256):
                    with Step(f"{dt} {tz}, int{int_type}"):
                        with By("converting datetime to string"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                        with And("computing the expected result using python"):
                            py_res = int(time.mktime(dt.timetuple()))
                            expected = f"{py_res}"
                            if int_type != 256:
                                """ALL dates fit into uint256, so no need to check"""
                                np_res = exec(f"np.uint{int_type}({py_res})")
                            else:
                                np_res = py_res
                        if np_res == py_res:
                            with Given(f"{py_res} fits int{int_type}"):
                                with When(
                                    f"making a query string for ClickHouse if py_res fits int{int_type}"
                                ):
                                    if cast:
                                        query = f"SELECT cast(toDateTime64('{dt_str}', 0, '{tz}'), 'UInt{int_type}')"
                                    else:
                                        query = f"SELECT toUInt{int_type}(toDateTime64('{dt_str}', 0, '{tz}'))"
                                with Then(f"I execute toInt{int_type}() query"):
                                    exec_query(request=query, expected=f"{expected}")


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toFloat_32_64_(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_float_32_64(self, cast):
    """Check the toFloat(32|64) functions with DateTime64 extended range"""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                for float_type in (32, 64):
                    with Step(f"{dt} {tz}, int{float_type}"):
                        with By("converting datetime to string"):
                            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                        with And("computing the expected result using python"):
                            py_res = int(time.mktime(dt.timetuple()))
                            expected = f"{py_res}"
                            np_res = exec(f"np.float{float_type}({py_res})")
                        if np_res == py_res:
                            with When(f"making a query string for ClickHouse"):
                                if cast:
                                    query = f"SELECT cast(toDateTime64('{dt_str}', 0, '{tz}'), 'Float{float_type}')"
                                else:
                                    query = f"SELECT toFloat{float_type}(toDateTime64('{dt_str}', 0, '{tz}'))"
                            with Then(f"I execute toFloat{float_type}() query"):
                                exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64_FromString_MissingTime(
        "1.0"
    )
)
def to_datetime64_from_string_missing_time(self):
    """Check the toDateTime64() with DateTime64 extended range conversion when string is missing the time part."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with Step(f"{dt} {tz}"):
                    with By("converting datetime to string"):
                        dt_str = dt.strftime("%Y-%m-%d")
                    with And("figure out expected result in python"):
                        expected = f"{dt.strftime('%Y-%m-%d')} 00:00:00"
                    with When(f"making a query string for ClickHouse"):
                        query = f"SELECT toDateTime64('{dt_str}', 0, '{tz}')"
                    with Then(f"I execute toDateTime64() query"):
                        exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime64("1.0")
)
def to_datetime64(self):
    """Check the toDateTime64() conversion with DateTime64. This is supposed to work in normal range ONLY."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with By("converting datetime to string"):
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                with And(f"making a query string for ClickHouse"):
                    query = f"SELECT toDateTime64('{dt_str}', 0, '{tz}')"
                with When("figure out expected result in python"):
                    expected = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}"
                with Then(f"I execute toDateTime64() query"):
                    exec_query(request=query, expected=f"{expected}")


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDate(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_date(self, cast):
    """Check the toDate() conversion with DateTime64. This is supposed to work in normal range ONLY."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with Step(f"{dt} {tz}"):
                    expected = None  # by default - not checked, checking the exitcode
                    with By("converting datetime to string"):
                        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

                    if in_normal_range(dt):
                        with And("DateTime64 fits normal range, change its value"):
                            expected = f"{dt.strftime('%Y-%m-%d')}"  # being checked in case DateTime64 fits normal range

                    with Given(f"I make a query string for ClickHouse"):
                        if cast:
                            query = f"SELECT CAST(toDateTime64('{dt_str}', 0, '{tz}'), 'Date')"
                        else:
                            query = (
                                f"SELECT toDate(toDateTime64('{dt_str}', 0, '{tz}'))"
                            )

                    with Then(f"I execute toDate() query and check return/exitcode"):
                        exec_query(request=query, expected=expected, exitcode=0)


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDateTime(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_datetime(self, cast):
    """Check the toDateTime() conversion with DateTime64. This is supposed to work in normal range ONLY."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with By("converting datetime to string"):
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                with And(f"making a query string for ClickHouse"):
                    if cast:
                        query = f"SELECT CAST(toDateTime64('{dt_str}', 0, '{tz}'), 'DateTime')"
                        with When("figure out expected result in python"):
                            dt_local = pytz.timezone(tz).localize(dt)
                            dt_transformed = dt_local.astimezone(tzlocal())
                            expected = f"{dt_transformed.strftime('%Y-%m-%d %H:%M:%S')}"
                    else:
                        query = (
                            f"SELECT toDateTime(toDateTime64('{dt_str}', 0, '{tz}'))"
                        )
                        with When("figure out expected result in python"):
                            expected = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}"

                if not in_normal_range(dt):
                    with When(f"I execute toDateTime() query out of normal range"):
                        exec_query(request=query, exitcode=0)
                else:
                    with When(f"I execute toDateTime() query"):
                        exec_query(request=query, expected=f"{expected}")


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toString(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_string(self, cast):
    """Check the toString() with DateTime64 extended range."""
    stress = self.context.stress
    timezones = timezones_range(stress)

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with Step(f"{dt} {tz}"):
                    with By("converting datetime to string"):
                        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    with When(f"making a query string for ClickHouse"):
                        if cast:
                            query = f"SELECT cast(toDateTime64('{dt_str}', 0, '{tz}'), 'String')"
                        else:
                            query = (
                                f"SELECT toString(toDateTime64('{dt_str}', 0, '{tz}'))"
                            )
                    with Then(f"I execute toDateTime64() query"):
                        exec_query(request=query, expected=f"{dt_str}")


def valid_decimal_range(bit_depth, S):
    """A helper to find valid range for Decimal(32|64|128|256) with given scale (S)"""
    return {
        32: -1 * 10 ** (9 - S),
        64: -1 * 10 ** (18 - S),
        128: -1 * 10 ** (38 - S),
        256: -1 * 10 ** (76 - S),
    }[bit_depth]


@TestOutline(Scenario)
@Examples(
    "cast",
    [
        (
            False,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toDecimal_32_64_128_256_(
                    "1.0"
                )
            ),
        ),
        (
            True,
            Requirements(
                RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_CAST_x_T_(
                    "1.0"
                )
            ),
        ),
    ],
)
def to_decimal_32_64_128_256(self, cast):
    """Check the toDecimal(32|64|128|256) functions with DateTime64 extended range.
    Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
    Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
    Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )
    Decimal256(S) - ( -1 * 10^(76 - S), 1 * 10^(76 - S) )
    """
    stress = self.context.stress
    timezones = timezones_range(stress)
    scales = {32: 9, 64: 18, 128: 38, 256: 76}

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                for decimal_type in (32, 64, 128, 256):
                    for scale in range(scales[decimal_type]):
                        with When(f"{dt} {tz}, Decimal{decimal_type}({scale})"):
                            valid_range = valid_decimal_range(
                                bit_depth=decimal_type, S=scale
                            )
                            with By("computing the expected result using python"):
                                expected = decimal.Decimal(time.mktime(dt.timetuple()))
                            if -valid_range < expected < valid_range:
                                with And("converting datetime to string"):
                                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                                with When(f"making a query string for ClickHouse"):
                                    if cast:
                                        query = f"SELECT cast(toDateTime64('{dt_str}', 0, '{tz}'), 'Decimal({decimal_type}, {scale})')"
                                    else:
                                        query = f"SELECT toDecimal{decimal_type}(toDateTime64('{dt_str}', 0, '{tz}'))"
                                with Then(f"I execute toDecimal{decimal_type}() query"):
                                    exec_query(request=query, expected=f"{expected}")


@TestOutline
def to_unix_timestamp64_milli_micro_nano(self, scale):
    """Check the toUnixTimestamp64[Milli/Micro/Nano] functions with DateTime64 extended range.
    :param scale: 3 for milli, 6 for micro, 9 for nano; int
    """
    stress = self.context.stress
    timezones = timezones_range(stress)
    func = {3: "Milli", 6: "Micro", 9: "Nano"}

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(
                year=year, stress=stress, microseconds=True
            )

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                with When(f"{dt} {tz}"):
                    with By("converting datetime to string"):
                        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                    with And("converting DateTime to UTC"):
                        dt = dt.astimezone(pytz.timezone("UTC"))
                    with And("computing the expected result using python"):
                        expected = int(dt.timestamp() * (10**scale))
                        if expected >= 0:
                            expected += dt.microsecond * 10 ** (scale - 6)
                        else:
                            expected -= dt.microsecond * 10 ** (scale - 6)
                    with When(f"making a query string for ClickHouse"):
                        query = f"SELECT toUnixTimestamp64{func[scale]}(toDateTime64('{dt_str}', {scale}, '{tz}'))"
                    with Then(f"I execute toUnixTimestamp64{func[scale]}() query"):
                        exec_query(request=query, expected=f"{expected}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Milli(
        "1.0"
    )
)
def to_unix_timestamp64_milli(self):
    """Check the toUnixTimestamp64Milli functions with DateTime64 extended range."""
    to_unix_timestamp64_milli_micro_nano(scale=3)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Micro(
        "1.0"
    )
)
def to_unix_timestamp64_micro(self):
    """Check the toUnixTimestamp64Micro functions with DateTime64 extended range."""
    to_unix_timestamp64_milli_micro_nano(scale=6)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_toUnixTimestamp64Nano(
        "1.0"
    )
)
def to_unix_timestamp64_nano(self):
    """Check the toUnixTimestamp64Nano functions with DateTime64 extended range."""
    to_unix_timestamp64_milli_micro_nano(scale=9)


@TestOutline
def from_unix_timestamp64_milli_micro_nano(self, scale):
    """Check the fromUnixTimestamp64[Milli/Micro/Nano] functions with DateTime64 extended range.
    :param scale: 3 for milli, 6 for micro, 9 for nano; int
    """
    stress = self.context.stress
    timezones = timezones_range(stress)
    func = {3: "Milli", 6: "Micro", 9: "Nano"}

    for year in years_range(stress):
        with Given(f"I select datetimes in {year}"):
            datetimes = select_dates_in_year(
                year=year, stress=stress, microseconds=True
            )

        for d in datetimes:
            for tz in timezones:
                dt = pytz.timezone(tz).localize(d)
                with When(f"{dt} {tz}"):
                    with By("converting datetime to string"):
                        d_str = d.strftime("%Y-%m-%d %H:%M:%S.%f")
                        d_str += "0" * (scale - 3)
                    with And("converting DateTime64 to UTC"):
                        dt = dt.astimezone(pytz.timezone("UTC"))
                    with And("computing the expected result using python"):
                        ts = int(dt.timestamp() * (10**scale))
                        if ts >= 0:
                            ts += dt.microsecond * 10 ** (scale - 6)
                        else:
                            ts -= dt.microsecond * 10 ** (scale - 6)
                    with And(f"making a query string for ClickHouse"):
                        query = f"SELECT fromUnixTimestamp64{func[scale]}(CAST({ts}, 'Int64'), '{tz}')"
                    with Then(f"I execute fromUnixTimestamp64{func[scale]}() query"):
                        exec_query(request=query, expected=f"{d_str}")


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Milli(
        "1.0"
    )
)
def from_unix_timestamp64_milli(self):
    """Check the fromUnixTimestamp64Milli functions with DateTime64 extended range."""
    from_unix_timestamp64_milli_micro_nano(scale=3)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Micro(
        "1.0"
    )
)
def from_unix_timestamp64_micro(self):
    """Check the fromUnixTimestamp64Micro functions with DateTime64 extended range."""
    from_unix_timestamp64_milli_micro_nano(scale=6)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions_fromUnixTimestamp64Nano(
        "1.0"
    )
)
def from_unix_timestamp64_nano(self):
    """Check the fromUnixTimestamp64Nano functions with DateTime64 extended range."""
    from_unix_timestamp64_milli_micro_nano(scale=9)


@TestFeature
@Requirements(RQ_SRS_010_DateTime64_ExtendedRange_TypeConversionFunctions("1.0"))
def type_conversion(self, node="clickhouse1"):
    """Check the type conversion operations with DateTime64.
    Cast can be set as Requirement thereby as the module
    tests exactly what CAST does.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario)
