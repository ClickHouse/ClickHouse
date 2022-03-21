from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *
from extended_precision_data_types.errors import *

funcs = [
    ('bitAnd', True, None),
    ('bitOr', True, None),
    ('bitXor', True, None),
    ('bitShiftLeft', True, None),
    ('bitShiftRight', True, None),
    ('bitRotateLeft', False, not_implemented_bigints('Bit rotate')),
    ('bitRotateRight', False, not_implemented_bigints('Bit rotate')),
    ('bitTest', False, not_implemented_bigints('bitTest')),
    ('bitTestAll', False, illegal_column()),
    ('bitTestAny', False, illegal_column()),
    ('bitNot', True, None),
    ('bitCount', True, None)
]

Examples_list =  [tuple(list(func)+list(data_type)+[Name(f'{func[0]} - {data_type[0]}')]) for func in funcs for data_type in data_types]
Examples_dec_list =  [tuple(list(func)+[Name(f'{func[0]} - Decimal256')]) for func in funcs]

@TestOutline(Scenario)
@Examples('func supported error int_type min max', Examples_list)
def bit_int_inline(self, func, supported, error, int_type, min, max, node=None):
    """ Check bit functions with Int128, UInt128, Int256, and UInt256 using inline tests.
    """

    if error is not None:
        exitcode,message = error

    if node is None:
        node = self.context.node

    if func in ["bitNot", "bitCount"]:

        with When(f"Check {func} with {int_type}"):
            execute_query(f"""
                SELECT {func}(to{int_type}(1)), {func}(to{int_type}(\'{max}\')), {func}(to{int_type}(\'{min}\'))
                """)

    elif supported:

        with When(f"I check {func} with {int_type}"):
            execute_query(f"""
                SELECT {func}(to{int_type}(1), 1), {func}(to{int_type}(\'{max}\'), 1), {func}(to{int_type}(\'{min}\'), 1)
                """)

    else:

        with When(f"I check {func} with {int_type}"):
            node.query(f"SELECT {func}(to{int_type}(1), 1), {func}(to{int_type}(\'{max}\'), 1), {func}(to{int_type}(\'{min}\'), 1)",
                exitcode=exitcode, message = message)

@TestOutline(Scenario)
@Examples('func supported error int_type min max', Examples_list)
def bit_int_table(self, func, supported, error, int_type, min, max, node=None):
    """ Check bit functions with Int128, UInt128, Int256, and UInt256 using table tests.
    """

    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    if error is not None:
        exitcode,message = error

    with Given(f"I have a table"):
        table(name = table_name, data_type = int_type)

    if func in ["bitNot", "bitCount"]:

        for value in [1, min, max]:

            with When(f"I insert the output of {func} with {int_type} and {value}"):
                node.query(f"INSERT INTO {table_name} SELECT {func}(to{int_type}(\'{value}\'))")

        with Then(f"I check the table with values of {func} and {int_type}"):
            execute_query(f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """)

    elif supported:

        for value in [1, min, max]:

            with When(f"I insert the output of {func} with {int_type} and {value}"):
                node.query(f"INSERT INTO {table_name} SELECT {func}(to{int_type}(\'{value}\'), 1)")

        with Then(f"I check the table with values of {func} and {int_type}"):
            execute_query(f"""
                SELECT * FROM {table_name} ORDER BY a ASC
                """)

    else:

        for value in [1, min, max]:

            with When(f"I insert the output of {func} with {int_type} and {value}"):
                node.query(f"INSERT INTO {table_name} SELECT {func}(to{int_type}(\'{value}\'), 1)",
                    exitcode=exitcode, message=message)

@TestOutline(Scenario)
@Examples('func supported error', Examples_dec_list)
def bit_dec_inline(self, func, supported, error, node=None):
    """ Check bit functions with Decimal256 using inline tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    exitcode, message = illegal_type()

    if node is None:
        node = self.context.node

    if func in ["bitNot", "bitCount"]:

        with When(f"Check {func} with Decimal256"):
            node.query(f"SELECT {func}(toDecimal256(1,0)), {func}(toDecimal256(\'{max}\',0)), {func}(toDecimal256(\'{min}\',0))",
                exitcode=exitcode, message = message)

    else:

        with When(f"I check {func} with Decimal256"):
            node.query(f"SELECT {func}(toDecimal256(1,0), 1), {func}(toDecimal256(\'{max}\',0), 1), {func}(toDecimal256(\'{min}\',0), 1)",
                exitcode=exitcode, message = message)

@TestOutline(Scenario)
@Examples('func supported error', Examples_dec_list)
def bit_dec_table(self, func, supported, error, node=None):
    """ Check bit functions with Decimal256 using table tests.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    table_name = f"table_{getuid()}"
    exitcode, message = illegal_type()

    if node is None:
        node = self.context.node

    with Given(f"I have a table"):
        table(name = table_name, data_type = 'Decimal256(0)')

    if func in ["bitNot", "bitCount"]:

        for value in [1, min, max]:

            with When(f"I insert the output of {func} with Decimal256 and {value}"):
                node.query(f"INSERT INTO {table_name} SELECT {func}(toDecimal256(\'{value}\',0))",
                    exitcode=exitcode, message = message)

    else:

        for value in [1, min, max]:

            with When(f"I insert the output of {func} with Decimal256 and {value}"):
                node.query(f"INSERT INTO {table_name} SELECT {func}(toDecimal256(\'{value}\',0), 1)",
                    exitcode=exitcode, message=message)

@TestFeature
@Name("bit")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Bit_Int_Supported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Bit_Int_NotSupported("1.0"),
    RQ_SRS_020_ClickHouse_Extended_Precision_Bit_Dec_NotSupported("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that bit functions work with extended precision data types.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    with allow_experimental_bigint(self.context.node):
        Scenario(run=bit_int_inline)
        Scenario(run=bit_int_table)
        Scenario(run=bit_dec_inline)
        Scenario(run=bit_dec_table)
