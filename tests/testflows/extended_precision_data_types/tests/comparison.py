from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

funcs = [
    ('equals',),
    ('notEquals',),
    ('less',),
    ('greater',),
    ('lessOrEquals',),
    ('greaterOrEquals',)
]

Examples_list =  [tuple(list(func)+list(data_type)+[Name(f'{func[0]} - {data_type[0]}')]) for func in funcs for data_type in data_types]
Examples_list_dec =  [tuple(list(func)+[Name(f'{func[0]} - Decimal256')]) for func in funcs]

@TestOutline(Scenario)
@Examples('func int_type min max', Examples_list)
def comp_int_inline(self, func, int_type, min, max, node=None):
    """Check comparison functions with Int128, UInt128, Int256, and UInt256 using inline tests.
    """

    if node is None:
        node = self.context.node

    with When(f"I check {func} with {int_type}"):
        execute_query(f"""
            SELECT {func}(to{int_type}(1), to{int_type}(1)), {func}(to{int_type}(\'{max}\'), to{int_type}(\'{min}\'))
            """)

@TestOutline(Scenario)
@Examples('func int_type min max', Examples_list)
def comp_int_table(self, func, int_type, min, max, node=None):
    """Check comparison functions with Int128, UInt128, Int256, and UInt256 using table tests.
    """

    if node is None:
        node = self.context.node

    table_name = f'table_{getuid()}'

    with Given(f"I have a table"):
        table(name = table_name, data_type = int_type)

    for value in [1, max, min]:

        with When(f"I insert into a table the output {func} with {int_type} and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func}(to{int_type}(\'{value}\'), to{int_type}(1))")

    with Then(f"I check the table for the output of {func} with {int_type}"):
        execute_query(f"""
            SELECT * FROM {table_name} ORDER BY a ASC
            """)

@TestOutline(Scenario)
@Examples('func', Examples_list_dec)
def comp_dec_inline(self, func, node=None):
    """Check comparison functions with Decimal256 using inline tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    with When(f"I check {func} with Decimal256"):
        execute_query(f"""
            SELECT {func}(toDecimal256(1,0), toDecimal256(1,0)), {func}(toDecimal256(\'{max}\',0), toDecimal256(\'{min}\',0))
            """)

@TestOutline(Scenario)
@Examples('func', Examples_list_dec)
def comp_dec_table(self, func, node=None):
    """Check comparison functions with Decimal256 using table tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    table_name = f'table_{getuid()}'

    with Given(f"I have a table"):
        table(name = table_name, data_type = 'Decimal256(0)')

    for value in [1, max, min]:

        with When(f"I insert into a table the output {func} with Decimal256 and {value}"):
            node.query(f"INSERT INTO {table_name} SELECT {func}(toDecimal256(\'{value}\',0), toDecimal256(1,0))")

    with Then(f"I check the table for the output of {func} with Decimal256"):
        execute_query(f"""
            SELECT * FROM {table_name} ORDER BY a ASC
            """)

@TestFeature
@Name("comparison")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Comparison("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that comparison functions work with extended precision data types.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=comp_int_inline)
        Scenario(run=comp_int_table)
        Scenario(run=comp_dec_inline)
        Scenario(run=comp_dec_table)
