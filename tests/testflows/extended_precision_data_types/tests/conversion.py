import os
import textwrap

from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

@contextmanager
def dictionary(name, node, mysql_node):
    """Create a table in MySQL and use it a source for a dictionary.
    """
    try:
        with Given("table in MySQL"):
            sql = f"""
                CREATE TABLE {name}(
                    id INT NOT NULL AUTO_INCREMENT,
                    int128 BIGINT,
                    uint128 BIGINT,
                    int256 BIGINT,
                    uint256 BIGINT,
                    dec256 DECIMAL,
                    PRIMARY KEY ( id )
                );
                """
            with When("I drop the table if exists"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)
            with And("I create a table"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        with And("dictionary that uses MySQL table as the external source"):
            with When("I drop the dictionary if exists"):
                node.query(f"DROP DICTIONARY IF EXISTS dict_{name}")
            with And("I create the dictionary"):
                sql = f"""
                CREATE DICTIONARY dict_{name}
                (
                    id UInt8,
                    int128 Int128,
                    uint128 UInt128,
                    int256 Int256,
                    uint256 UInt256,
                    dec256 Decimal256(0)
                )
                PRIMARY KEY id
                SOURCE(MYSQL(
                    USER 'user'
                    PASSWORD 'password'
                    DB 'db'
                    TABLE '{name}'
                    REPLICA(PRIORITY 1 HOST '{mysql_node.name}' PORT 3306)
                ))
                LAYOUT(HASHED())
                LIFETIME(0)
                """
                node.query(textwrap.dedent(sql))

        yield f"dict_{name}"

    finally:
        with Finally("I drop the dictionary", flags=TE):
            node.query(f"DROP DICTIONARY IF EXISTS dict_{name}")

        with And("I drop a table in MySQL", flags=TE):
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)

@contextmanager
def table(name, node, mysql_node):
    """Create a table in MySQL and use it a source for a table in ClickHouse.
    """
    try:
        with Given("table in MySQL"):
            sql = f"""
                CREATE TABLE {name}(
                    id INT NOT NULL AUTO_INCREMENT,
                    int128 BIGINT,
                    uint128 BIGINT,
                    int256 BIGINT,
                    uint256 BIGINT,
                    dec256 DECIMAL,
                    PRIMARY KEY ( id )
                );
                """
            with When("I drop the table if exists"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)

            with And("I create a table"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        with And("table that uses MySQL table as the external source"):

            with When("I drop the table if exists"):
                node.query(f"DROP TABLE IF EXISTS {name}")
            with And("I create the table"):
                sql = f"""
                CREATE TABLE {name}
                (
                    id UInt8,
                    int128 Int128,
                    uint128 UInt128,
                    int256 Int256,
                    uint256 UInt256,
                    dec256 Decimal256(0)
                )
                ENGINE = MySQL('{mysql_node.name}:3306', 'default', '{name}', 'default', 'password')
                """
                node.query(textwrap.dedent(sql))

        yield f"table_{name}"

    finally:
        with Finally("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {name}")

        with And("I drop a table in MySQL", flags=TE):
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)

@contextmanager
def table_func(name, node, mysql_node):
    """Create a table in MySQL and use it a source for a table using mysql table function.
    """
    try:
        with Given("table in MySQL"):
            sql = f"""
                CREATE TABLE {name}(
                    id INT NOT NULL AUTO_INCREMENT,
                    int128 BIGINT,
                    uint128 BIGINT,
                    int256 BIGINT,
                    uint256 BIGINT,
                    dec256 DECIMAL,
                    PRIMARY KEY ( id )
                );
                """
            with When("I drop the table if exists"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)
            with And("I create a table"):
                mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        yield f"mysql('{mysql_node.name}:3306', 'db', '{name}', 'user', 'password')"

    finally:

        with Finally("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {name}")

        with And("I drop a table in MySQL", flags=TE):
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"DROP TABLE IF EXISTS {name};\"", exitcode=0)

@TestOutline(Scenario)
@Examples('int_type min max',[
    ('Int128', '-170141183460469231731687303715884105728', '170141183460469231731687303715884105727', Requirements(RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_toInt128("1.0")), Name('Int128')),
    ('Int256', '-57896044618658097711785492504343953926634992332820282019728792003956564819968', '57896044618658097711785492504343953926634992332820282019728792003956564819967', Requirements(RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_toInt256("1.0")), Name('Int256')),
    ('UInt128','0','340282366920938463463374607431768211455', Requirements(RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_toUInt128("1.0")), Name('UInt128')),
    ('UInt256', '0', '115792089237316195423570985008687907853269984665640564039457584007913129639935', Requirements(RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_toUInt256("1.0")), Name('UInt256')),
])
def int_conversion(self, int_type, min, max, node=None):
    """Check that ClickHouse converts values to Int128.
    """

    if node is None:
        node = self.context.node

    with When(f"I convert {min}, {max}, 1 to {int_type}"):
        output = node.query(f"SELECT to{int_type}(\'{min}\'), to{int_type}(\'{max}\'), to{int_type}(1) format TabSeparatedRaw").output
        assert output == f'{min}\t{max}\t1', error()

@TestScenario
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_toDecimal256("1.0"),
)
def to_decimal256(self, node=None):
    """Check that ClickHouse converts values to Int128.
    """
    min = Decimal256_min_max[0]
    max = Decimal256_min_max[1]

    if node is None:
        node = self.context.node

    with When(f"I check toDecimal256 with 0 scale with 1, {max}, and {min}"):

        for value in [1,min,max]:
            output = node.query(f"SELECT toDecimal256(\'{value}\',0)").output
            assert output == str(value), error()

    for scale in range(1,76):

        with When(f"I check toDecimal256 with {scale} scale with its max"):
            output = node.query(f"SELECT toDecimal256(\'{10**(76-scale)-1}\',{scale})").output
            assert float(output) == float(10**(76-scale)-1), error()

        with And(f"I check toDecimal256 with {scale} scale with its min"):
            output = node.query(f"SELECT toDecimal256(\'{-10**(76-scale)+1}\',{scale})").output
            assert float(output) == float(-10**(76-scale)+1), error()

@TestScenario
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_ToMySQL("1.0"),
)
def MySQL_table(self, node=None):
    """Check that ClickHouse converts MySQL values from MySQL table into ClickHouse table.
    """
    table_name = f'table_{getuid()}'

    node = self.context.node
    mysql_node = self.context.mysql_node

    with table(table_name, node, mysql_node):

        with When("I insert parameters values in MySQL"):
            sql = f"""
                INSERT INTO {table_name}(int128, uint128, int256, uint256, dec256) VALUES (1,1,1,1,1);
            """
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        with Then("I select from the table on top of the mysql table"):
            node.query(f"SELECT * FROM {table_name}",
                exitcode=50, message='Exception:')

@TestScenario
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_FromMySQL("1.0"),
)
def MySQL_func(self, node=None):
    """Check that ClickHouse converts MySQL values into a ClickHouse table using the MySQL table function.
    """
    table_name = f'table_{getuid()}'

    node = self.context.node
    mysql_node = self.context.mysql_node

    with table_func(table_name, node, mysql_node) as table_function:

        with When("I insert parameters values in MySQL"):
            sql = f"""
                INSERT INTO {table_name}(int128, uint128, int256, uint256, dec256) VALUES (1,1,1,1,1);
            """
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        with And("I make sure the table doesn't exist"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with And("I create the table"):
            node.query(f"CREATE TABLE {table_name} (id UInt8, int128 Int128, uint128 UInt128, int256 Int256, uint256 UInt256, dec256 Decimal256(0)) Engine = Memory")

        with And("I insert into the clickhouse table from the mysql table"):
            node.query(f"INSERT INTO {table_name} SELECT * FROM {table_function}")

        with Then("I select from the clickhouse table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert output ==  '1\t1\t1\t1\t1\t1', error()

@TestScenario
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Conversion_ToMySQL("1.0"),
)
def MySQL_dict(self, node=None):
    """Check that ClickHouse converts MySQL values from MySQL table into ClickHouse dictionary.
    """

    node = self.context.node
    mysql_node = self.context.mysql_node

    table_name = f'table_{getuid()}'

    with dictionary(table_name, node, mysql_node):

        with When("I insert parameters values in MySQL"):
            sql = f"""
                INSERT INTO {table_name}(int128, uint128, int256, uint256, dec256) VALUES (1,1,1,1,1);
            """
            mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

        with Then("I select from the table on top of the mysql table"):
            node.query(f"SELECT * FROM dict_{table_name}",
                exitcode=50, message='Exception:')

@TestFeature
@Name("conversion")
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check the conversion of extended precision data types.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    for scenario in loads(current_module(), Scenario):
        with allow_experimental_bigint(self.context.node):
            Scenario(run=scenario)
