from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

funcs = [
    ("exp(", 3, 0),
    ("log(", 0, 0),
    ("ln(", 0, 0),
    ("exp2(", 2, 0),
    ("log2(", 0, 0),
    ("exp10(", 10, 0),
    ("log10(", 0, 0),
    ("sqrt(", 1, 0),
    ("cbrt(", 1, 0),
    ("erf(", 1, 0),
    ("erfc(", 0, 0),
    ("lgamma(", 0, 0),
    ("tgamma(", 1, 0),
    ("sin(", 1, 0),
    ("cos(", 1, 0),
    ("tan(", 2, 0),
    ("asin(", 2, 0),
    ("acos(", 0, 0),
    ("atan(", 1, 0),
    ("intExp2(", 2, 48),
    ("intExp10(", 10, 48),
    ("cosh(", 2, 0),
    ("acosh(", 0, 0),
    ("sinh(", 1, 0),
    ("asinh(", 1, 0),
    ("tanh(", 1, 0),
    ("atanh(", "inf", 0),
    ("log1p(", 1, 0),
    ("sign(", 1, 0),
    ("pow(1,", 1, 43),
    ("power(1,", 1, 43),
    ("atan2(1,", 1, 43),
    ("hypot(1,", 1, 43),
]

Examples_list = [
    tuple(list(func) + list(data_type) + [Name(f"{func[0]}) - {data_type[0]}")])
    for func in funcs
    for data_type in data_types
]
Examples_dec_list = [
    tuple(list(func) + [Name(f"{func[0]}) - Decimal256")]) for func in funcs
]


@TestOutline(Scenario)
@Examples("func expected_result exitcode int_type min max", Examples_list)
def math_int_inline(
    self, func, expected_result, exitcode, int_type, min, max, node=None
):
    """Check mathematical functions with Int128, UInt128, Int256, and UInt256 using inline tests."""
    if node is None:
        node = self.context.node

    if func in ["intExp2(", "intExp10(", "pow(1,", "power(1,", "atan2(1,", "hypot(1,"]:

        with When(f"I check {func} with {int_type} using 1, max, and min"):
            node.query(
                f"SELECT {func} to{int_type}(1)), {func} to{int_type}('{max}')), {func} to{int_type}('{min}'))",
                exitcode=exitcode,
                message="Exception:",
            )

    else:

        with When(f"I check {func} with {int_type} using 1"):
            output = node.query(f"SELECT {func} to{int_type}(1))").output
            if output == "inf":
                pass
            else:
                assert round(float(output)) == expected_result, error()

        with And(f"I check {func} with {int_type} using max and min"):
            execute_query(
                f"""
                SELECT round({func} to{int_type}(\'{max}\')), {rounding_precision}), round({func} to{int_type}(\'{min}\')), {rounding_precision})
                """
            )


@TestOutline(Scenario)
@Examples("func expected_result exitcode int_type min max", Examples_list)
def math_int_table(
    self, func, expected_result, exitcode, int_type, min, max, node=None
):
    """Check mathematical functions with Int128, UInt128, Int256, and UInt256 using table tests."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given(f"I have a table"):
        table(name=table_name, data_type=f"Nullable({int_type})")

    if func in ["intExp2(", "intExp10(", "pow(1,", "power(1,", "atan2(1,", "hypot(1,"]:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with {int_type} using {value} into a table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func} to{int_type}('{value}'))",
                    exitcode=exitcode,
                    message="Exception:",
                )

    else:

        for value in [1, max, min]:

            with And(
                f"I insert the output of {func} with {int_type} using {value} into a table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT round(to{int_type}OrZero( toString({func} to{int_type}('{value}')))), {rounding_precision})"
                )

        with Then(f"I check the outputs of {func} with {int_type}"):
            execute_query(
                f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """
            )


@TestOutline(Scenario)
@Examples("func expected_result exitcode", Examples_dec_list)
def math_dec_inline(self, func, expected_result, exitcode, node=None):
    """Check mathematical functions with Decimal256 using inline tests."""
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    if func in ["intExp2(", "intExp10(", "pow(1,", "power(1,", "atan2(1,", "hypot(1,"]:

        with When(f"I check {func} with Decimal256 using 1, max, and min"):
            node.query(
                f"SELECT {func} toDecimal256(1,0)), {func} toDecimal256('{max}',0)), {func} toDecimal256('{min}',0))",
                exitcode=43,
                message="Exception: Illegal type ",
            )

    else:

        with When(f"I check {func} with Decimal256 using 1"):
            output = node.query(f"SELECT {func} toDecimal256(1,0))").output
            if output == "inf":
                pass
            else:
                assert round(float(output)) == expected_result, error()

        with And(f"I check {func} with Decimal256 using max and min"):
            execute_query(
                f"""
                SELECT round({func} toDecimal256(\'{max}\',0)),{rounding_precision}), round({func} toDecimal256(\'{min}\',0)),{rounding_precision})
                """
            )


@TestOutline(Scenario)
@Examples("func expected_result exitcode", Examples_dec_list)
def math_dec_table(self, func, expected_result, exitcode, node=None):
    """Check mathematical functions with Decimal256 using table tests."""
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given(f"I have a table"):
        table(name=table_name, data_type="Decimal256(0)")

    if func in ["intExp2(", "intExp10(", "pow(1,", "power(1,", "atan2(1,", "hypot(1,"]:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with Decimal256 using {value} into a table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func} toDecimal256('{value}',0))",
                    exitcode=43,
                    message="Exception: Illegal type ",
                )

    else:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with Decimal256 using {value} into a table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT round(toDecimal256OrZero( toString({func} toDecimal256('{value}',0))),0), 7)"
                )

        with Then(f"I check the outputs of {func} with Decimal256"):
            execute_query(
                f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """
            )


@TestFeature
@Name("mathematical")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Mathematical_Supported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Mathematical_NotSupported("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that mathematical functions work with extended precision data types."""
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=math_int_inline)
        Scenario(run=math_int_table)
        Scenario(run=math_dec_inline)
        Scenario(run=math_dec_table)
