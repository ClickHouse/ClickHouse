from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

funcs = [
    ("ceil", 1, True),
    ("floor", 1, True),
    ("trunc", 1, True),
    ("round", 1, True),
    ("roundBankers", 1, True),
    ("roundToExp2", 1, False),
    ("roundDuration", 1, True),
    ("roundAge", 17, True),
    ("roundDown", 1, False),
]

Examples_list = [
    tuple(list(func) + list(data_type) + [Name(f"{func[0]} - {data_type[0]}")])
    for func in funcs
    for data_type in data_types
]
Examples_dec_list = [
    tuple(list(func) + [Name(f"{func[0]} - Decimal256")]) for func in funcs
]


@TestOutline(Scenario)
@Examples("func expected_result supported int_type min max", Examples_list)
def round_int_inline(
    self, func, expected_result, supported, int_type, min, max, node=None
):
    """Check rounding functions with Int128, UInt128, Int256, and UInt256 using inline tests."""

    if node is None:
        node = self.context.node

    if func == "roundDown":

        with When(f"I check roundDown with {int_type}"):
            node.query(
                f"SELECT roundDown(to{int_type}(1), [0,2]), roundDown(to{int_type}('{max}'), [0,2]), roundDown(to{int_type}('{min}'), [0,2])",
                exitcode=44,
                message=f"Exception: Illegal column {int_type} of first argument of function roundDown",
            )

    elif supported:

        with When(f"I check {func} with {int_type}"):
            output = node.query(f"SELECT {func}(to{int_type}(1))").output
            assert output == str(expected_result), error()

        with And(f"I check {func} with {int_type} using min and max values"):
            execute_query(
                f"""
                SELECT {func}(to{int_type}(\'{min}\')), {func}(to{int_type}(\'{max}\'))
                """
            )

    else:

        with When(f"I check {func} with {int_type}"):
            node.query(
                f"SELECT {func}(to{int_type}(1)), {func}(to{int_type}('{max}')), {func}(to{int_type}('{min}'))",
                exitcode=48,
                message=f"Exception: {func}() for big integers is not implemented:",
            )


@TestOutline(Scenario)
@Examples("func expected_result supported int_type min max", Examples_list)
def round_int_table(
    self, func, expected_result, supported, int_type, min, max, node=None
):
    """Check rounding functions with Int128, UInt128, Int256, and UInt256 using table tests."""

    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a table"):
        table(name=table_name, data_type=int_type)

    if func == "roundDown":

        for value in [1, max, min]:

            with When(f"I check roundDown with {int_type} and {value}"):
                node.query(
                    f"INSERT INTO {table_name} SELECT roundDown(to{int_type}('{value}'), [0,2])",
                    exitcode=44,
                    message=f"Exception: Illegal column {int_type} of first argument of function roundDown",
                )

    elif supported:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with {int_type} and {value} into the table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func}(to{int_type}('{value}'))"
                )

        with Then(f"I select the output of {func} with {int_type} from the table"):
            execute_query(
                f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """
            )

    else:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with {int_type} and {value} into the table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func}(to{int_type}(1))",
                    exitcode=48,
                    message=f"Exception: {func}() for big integers is not implemented:",
                )


@TestOutline(Scenario)
@Examples("func expected_result supported", Examples_dec_list)
def round_dec_inline(self, func, expected_result, supported, node=None):
    """Check rounding functions with Decimal256 using inline tests."""
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    if func == "roundDown":

        with When(f"I check roundDown with Decimal256"):
            node.query(
                f"""SELECT roundDown(toDecimal256(1,0), [toDecimal256(0,0),toDecimal256(2,0)]),
                roundDown(toDecimal256(\'{max}\',0), [toDecimal256(0,0),toDecimal256(2,0)]),
                roundDown(toDecimal256(\'{min}\',0), [toDecimal256(0,0),toDecimal256(2,0)])""",
                exitcode=44,
                message=f"Exception: Illegal column Decimal256 of first argument of function roundDown",
            )

    elif func not in ["roundDuration", "roundAge", "roundToExp2"]:

        with When(f"I check {func} with Decimal256"):
            output = node.query(f"SELECT {func}(toDecimal256(1,0))").output
            assert output == str(expected_result), error()

        with And(f"I check {func} with Decimal256 using min and max values"):
            execute_query(
                f"""
                SELECT {func}(toDecimal256(\'{min}\',0)), {func}(toDecimal256(\'{max}\',0))
                """
            )

    else:

        with When(f"I check {func} with Decimal256"):
            node.query(
                f"SELECT {func}(toDecimal256(1,0)), {func}(toDecimal256('{max}',0)), {func}(toDecimal256('{min}',0))",
                exitcode=43,
                message=f"Exception: Illegal type Decimal(76, 0)",
            )


@TestOutline(Scenario)
@Examples("func expected_result supported", Examples_dec_list)
def round_dec_table(self, func, expected_result, supported, node=None):
    """Check rounding functions with Decimal256 using table tests."""
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a table"):
        table(name=table_name, data_type="Decimal256(0)")

    if func == "roundDown":

        for value in [1, max, min]:

            with When(f"I check roundDown with Decimal256 and {value}"):
                node.query(
                    f"INSERT INTO {table_name} SELECT roundDown(toDecimal256('{value}',0), [toDecimal256(0,0),toDecimal256(2,0)])",
                    exitcode=44,
                    message=f"Exception: Illegal column Decimal256 of first argument of function roundDown",
                )

    elif func not in ["roundDuration", "roundAge", "roundToExp2"]:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with Decimal256 and {value} into the table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func}(toDecimal256('{value}',0))"
                )

        with Then(f"I select the output of {func} with Decimal256 from the table"):
            execute_query(
                f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """
            )

    else:

        for value in [1, max, min]:

            with When(
                f"I insert the output of {func} with Decimal256 and {value} into the table"
            ):
                node.query(
                    f"INSERT INTO {table_name} SELECT {func}(toDecimal256('{value}',0))",
                    exitcode=43,
                    message=f"Exception: Illegal type Decimal(76, 0)",
                )


@TestFeature
@Name("rounding")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Rounding_Int_Supported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Rounding_Int_NotSupported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Rounding_Dec_Supported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Rounding_Dec_NotSupported("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that rounding functions work with extended precision data types."""
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=round_int_inline)
        Scenario(run=round_int_table)
        Scenario(run=round_dec_inline)
        Scenario(run=round_dec_table)
