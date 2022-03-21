from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

funcs = [
    ('and',),
    ('or',),
    ('not',),
    ('xor',),
]

Examples_list =  [tuple(list(func)+list(data_type)+[Name(f'{func[0]} - {data_type[0]}')]) for func in funcs for data_type in data_types]
Examples_list_dec =  [tuple(list(func)+[Name(f'{func[0]} - Decimal256')]) for func in funcs]

@TestOutline(Scenario)
@Examples('func int_type min max', Examples_list)
def log_int_inline(self, func, int_type, min, max, node=None):
    """Check logical functions with Int128, Int256, and UInt256 using inline tests.
    """
    table_name = f'table_{getuid()}'

    if node is None:
        node = self.context.node

    with When(f"Check {func} with {int_type}"):
        node.query(f"SELECT {func}(to{int_type}(1), to{int_type}(1)), {func}(to{int_type}(\'{max}\'), to{int_type}(1)), {func}(to{int_type}(\'{min}\'), to{int_type}(1))",
            exitcode=43, message = 'Exception: Illegal type ')

@TestOutline(Scenario)
@Examples('func int_type min max', Examples_list)
def log_int_table(self, func, int_type, min, max, node=None):
    """Check logical functions with Int128, Int256, and UInt256 using table tests.
    """

    if node is None:
        node = self.context.node

    table_name = f'table_{getuid()}'

    with Given(f"I have a table"):
        table(name = table_name, data_type = int_type)

    for value in [1, min, max]:

        with When(f"Check {func} with {int_type} and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func}(to{int_type}(\'{value}\'), to{int_type}(\'{value}\'))",
                exitcode=43, message = 'Exception: Illegal type')

@TestOutline(Scenario)
@Examples('func', funcs)
def log_dec_inline(self, func, node=None):
    """Check logical functions with Decimal256 using inline tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    with When(f"Check {func} with Decimal256"):
        node.query(f"SELECT {func}(toDecimal256(1,0), toDecimal256(1,0)), {func}(toDecimal256(\'{max}\',0), toDecimal256(1)), {func}(toDecimal256(\'{min}\',0), toDecimal256(1))",
            exitcode=43, message = 'Exception: Illegal type ')

@TestOutline(Scenario)
@Examples('func', funcs)
def log_dec_table(self, func, node=None):
    """Check logical functions with Decimal256 using table tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    table_name = f'table_{getuid()}'

    with Given(f"I have a table"):
        table(name = table_name, data_type = 'Decimal256(0)')

    for value in [1, min, max]:

        with When(f"Check {func} with Decimal256 and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func}(toDecimal256(\'{value}\',0), toDecimal256(\'{value}\',0))",
                exitcode=43, message = 'Exception: Illegal type ')

@TestFeature
@Name("logical")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Logical("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that comparison functions work with extended precision data types.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=log_int_inline)
        Scenario(run=log_int_table)
        Scenario(run=log_dec_inline)
        Scenario(run=log_dec_table)
