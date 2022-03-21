from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

funcs = [
    ('isNull(', 0),
    ('isNotNull(', 1),
    ('coalesce(', 1),
    ('assumeNotNull(', 1),
    ('toNullable(', 1),
    ('ifNull(1,', 1),
    ('nullIf(1,', '\\N'),
]

Examples_list =  [tuple(list(func)+list(data_type)+[Name(f'{func[0]}) - {data_type[0]}')]) for func in funcs for data_type in data_types]
Examples_list_dec =  [tuple(list(func)+[Name(f'{func[0]}) - Decimal256')]) for func in funcs]

@TestOutline(Scenario)
@Examples('func expected_result int_type min max', Examples_list)
def null_int_inline(self, func, expected_result, int_type, min, max, node=None):
    """Check null function with Int128, UInt128, Int256, and UInt256 using inline tests.
    """

    if node is None:
        node = self.context.node

    with When(f"I check {func} with {int_type}"):
        output = node.query(f"SELECT {func} to{int_type}(1))").output
        assert output == str(expected_result), error()

    with And(f"I check {func} with {int_type} using min and max"):
        execute_query(f"""
            SELECT {func} to{int_type}(\'{min}\')), {func} to{int_type}(\'{max}\'))
            """)

@TestOutline(Scenario)
@Examples('func expected_result int_type min max', Examples_list)
def null_int_table(self, func, expected_result, int_type, min, max, node=None):
    """Check null function with Int128, UInt128, Int256, and UInt256 using table tests.
    """

    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a table"):
        table(name = table_name, data_type = f'Nullable({int_type})')

    for value in [1, min, max]:

        with When(f"I insert the output of {func} with {int_type} and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func} to{int_type}(\'{value}\'))")

    with Then(f"I check {func} with {int_type} on the table"):
        execute_query(f"""
            SELECT * FROM {table_name} ORDER BY a ASC
            """)

@TestOutline(Scenario)
@Examples('func expected_result', Examples_list_dec)
def null_dec_inline(self, func, expected_result, node=None):
    """Check null function with Decimal256 using inline tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    with When(f"I check {func} with Decimal256"):
        output = node.query(f"SELECT {func} toDecimal256(1,0))").output
        assert output == str(expected_result), error()

    with And(f"I check {func} with Decimal256 using min and max"):
        execute_query(f"""
            SELECT {func} toDecimal256(\'{min}\',0)), {func} toDecimal256(\'{max}\',0))
            """)

@TestOutline(Scenario)
@Examples('func expected_result', Examples_list_dec)
def null_dec_table(self, func, expected_result, node=None):
    """Check null function with Decimal256 using table tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a table"):
        table(name = table_name, data_type = 'Nullable(Decimal256(0))')

    for value in [1, min, max]:

        with When(f"I insert the output of {func} with Decimal256 and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func} toDecimal256(\'{value}\',0))")

    with Then(f"I check {func} with Decimal256 on the table"):
        execute_query(f"""
            SELECT * FROM {table_name} ORDER BY a ASC
            """)

@TestFeature
@Name("null")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Null("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that null functions work with extended precision data types.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=null_int_inline)
        Scenario(run=null_int_table)
        Scenario(run=null_dec_inline)
        Scenario(run=null_dec_table)
