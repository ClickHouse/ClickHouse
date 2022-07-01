import os
import re
import uuid
import tempfile

from testflows.core import *
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType
from testflows.asserts import values, error, snapshot


def window_frame_error():
    return (36, "Exception: Window frame")


def frame_start_error():
    return (36, "Exception: Frame start")


def frame_end_error():
    return (36, "Exception: Frame end")


def frame_offset_nonnegative_error():
    return syntax_error()


def frame_end_unbounded_preceding_error():
    return (36, "Exception: Frame end cannot be UNBOUNDED PRECEDING")


def frame_range_offset_error():
    return (48, "Exception: The RANGE OFFSET frame")


def frame_requires_order_by_error():
    return (
        36,
        "Exception: The RANGE OFFSET window frame requires exactly one ORDER BY column, 0 given",
    )


def syntax_error():
    return (62, "Exception: Syntax error")


def groups_frame_error():
    return (48, "Exception: Window frame 'Groups' is not implemented")


def getuid():
    if current().subtype == TestSubType.Example:
        testname = (
            f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
        )
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace("-", "_")


def convert_output(s):
    """Convert expected output to TSV format."""
    return "\n".join(
        [
            l.strip()
            for i, l in enumerate(re.sub("\s+\|\s+", "\t", s).strip().splitlines())
            if i != 1
        ]
    )


def execute_query(
    sql, expected=None, exitcode=None, message=None, format="TabSeparatedWithNames"
):
    """Execute SQL query and compare the output to the snapshot."""
    name = basename(current().name)

    with When("I execute query", description=sql):
        r = current().context.node.query(
            sql + " FORMAT " + format, exitcode=exitcode, message=message
        )

    if message is None:
        if expected is not None:
            with Then("I check output against expected"):
                assert r.output.strip() == expected, error()
        else:
            with Then("I check output against snapshot"):
                with values() as that:
                    assert that(
                        snapshot(
                            "\n" + r.output.strip() + "\n",
                            "tests",
                            name=name,
                            encoder=str,
                        )
                    ), error()


@TestStep(Given)
def t1_table(self, name="t1", distributed=False):
    """Create t1 table."""
    table = None
    data = ["(1, 1)", "(1, 2)", "(2, 2)"]

    if not distributed:
        with By("creating table"):
            sql = """
                CREATE TABLE {name} (
                    f1 Int8,
                    f2 Int8
                ) ENGINE = MergeTree ORDER BY tuple()
                """
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            sql = f"INSERT INTO {name} VALUES {','.join(data)}"
            self.context.node.query(sql)

    else:
        with By("creating table"):
            sql = """
                CREATE TABLE {name} ON CLUSTER sharded_cluster (
                    f1 Int8,
                    f2 Int8
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') ORDER BY tuple()
                """
            create_table(
                name=name + "_source", statement=sql, on_cluster="sharded_cluster"
            )

        with And("a distributed table"):
            sql = (
                "CREATE TABLE {name} AS "
                + name
                + "_source"
                + " ENGINE = Distributed(sharded_cluster, default, "
                + f"{name + '_source'}, f1 % toUInt8(getMacro('shard')))"
            )
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            for row in data:
                sql = f"INSERT INTO {name} VALUES {row}"
                self.context.node.query(sql)

    return table


@TestStep(Given)
def datetimes_table(self, name="datetimes", distributed=False):
    """Create datetimes table."""
    table = None
    data = [
        "(1, '2000-10-19 10:23:54', '2000-10-19 10:23:54')",
        "(2, '2001-10-19 10:23:54', '2001-10-19 10:23:54')",
        "(3, '2001-10-19 10:23:54', '2001-10-19 10:23:54')",
        "(4, '2002-10-19 10:23:54', '2002-10-19 10:23:54')",
        "(5, '2003-10-19 10:23:54', '2003-10-19 10:23:54')",
        "(6, '2004-10-19 10:23:54', '2004-10-19 10:23:54')",
        "(7, '2005-10-19 10:23:54', '2005-10-19 10:23:54')",
        "(8, '2006-10-19 10:23:54', '2006-10-19 10:23:54')",
        "(9, '2007-10-19 10:23:54', '2007-10-19 10:23:54')",
        "(10, '2008-10-19 10:23:54', '2008-10-19 10:23:54')",
    ]

    if not distributed:
        with By("creating table"):
            sql = """
            CREATE TABLE {name} (
                id UInt32,
                f_timestamptz DateTime('CET'),
                f_timestamp DateTime
            ) ENGINE = MergeTree() ORDER BY tuple()
            """
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            sql = f"INSERT INTO {name} VALUES {','.join(data)}"
            self.context.node.query(sql)

    else:
        with By("creating table"):
            sql = """
            CREATE TABLE {name} ON CLUSTER sharded_cluster (
                id UInt32,
                f_timestamptz DateTime('CET'),
                f_timestamp DateTime
            ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') ORDER BY tuple()
            """
            create_table(
                name=name + "_source", statement=sql, on_cluster="sharded_cluster"
            )

        with And("a distributed table"):
            sql = (
                "CREATE TABLE {name} AS "
                + name
                + "_source"
                + " ENGINE = Distributed(sharded_cluster, default, "
                + f"{name + '_source'}, id % toUInt8(getMacro('shard')))"
            )
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            for row in data:
                sql = f"INSERT INTO {name} VALUES {row}"
                self.context.node.query(sql)

    return table


@TestStep(Given)
def numerics_table(self, name="numerics", distributed=False):
    """Create numerics tables."""
    table = None

    data = [
        "(0, '-infinity', '-infinity', toDecimal64(-1000,15))",
        "(1, -3, -3, -3)",
        "(2, -1, -1, -1)",
        "(3, 0, 0, 0)",
        "(4, 1.1, 1.1, 1.1)",
        "(5, 1.12, 1.12, 1.12)",
        "(6, 2, 2, 2)",
        "(7, 100, 100, 100)",
        "(8, 'infinity', 'infinity', toDecimal64(1000,15))",
        "(9, 'NaN', 'NaN', 0)",
    ]

    if not distributed:
        with By("creating a table"):
            sql = """
                CREATE TABLE {name} (
                    id Int32,
                    f_float4 Float32,
                    f_float8 Float64,
                    f_numeric Decimal64(15)
                ) ENGINE = MergeTree() ORDER BY tuple();
            """
            create_table(name=name, statement=sql)

        with And("populating table with data"):
            sql = f"INSERT INTO {name} VALUES {','.join(data)}"
            self.context.node.query(sql)

    else:
        with By("creating a table"):
            sql = """
                CREATE TABLE {name} ON CLUSTER sharded_cluster (
                    id Int32,
                    f_float4 Float32,
                    f_float8 Float64,
                    f_numeric Decimal64(15)
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') ORDER BY tuple();
            """
            create_table(
                name=name + "_source", statement=sql, on_cluster="sharded_cluster"
            )

        with And("a distributed table"):
            sql = (
                "CREATE TABLE {name} AS "
                + name
                + "_source"
                + " ENGINE = Distributed(sharded_cluster, default, "
                + f"{name + '_source'}, id % toUInt8(getMacro('shard')))"
            )
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            for row in data:
                sql = f"INSERT INTO {name} VALUES {row}"
                self.context.node.query(sql)

    return table


@TestStep(Given)
def tenk1_table(self, name="tenk1", distributed=False):
    """Create tenk1 table."""
    table = None

    if not distributed:
        with By("creating a table"):
            sql = """
            CREATE TABLE {name} (
                unique1             Int32,
                unique2             Int32,
                two                 Int32,
                four                Int32,
                ten                 Int32,
                twenty              Int32,
                hundred             Int32,
                thousand            Int32,
                twothousand         Int32,
                fivethous           Int32,
                tenthous            Int32,
                odd                 Int32,
                even                Int32,
                stringu1            String,
                stringu2            String,
                string4             String
            )  ENGINE = MergeTree() ORDER BY tuple()
            """
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            datafile = os.path.join(current_dir(), "tenk.data")
            debug(datafile)
            self.context.cluster.command(
                None,
                f'cat "{datafile}" | {self.context.node.cluster.docker_compose} exec -T {self.context.node.name} clickhouse client -q "INSERT INTO {name} FORMAT TSV"',
                exitcode=0,
            )
    else:
        with By("creating a table"):
            sql = """
            CREATE TABLE {name} ON CLUSTER sharded_cluster (
                unique1             Int32,
                unique2             Int32,
                two                 Int32,
                four                Int32,
                ten                 Int32,
                twenty              Int32,
                hundred             Int32,
                thousand            Int32,
                twothousand         Int32,
                fivethous           Int32,
                tenthous            Int32,
                odd                 Int32,
                even                Int32,
                stringu1            String,
                stringu2            String,
                string4             String
            )  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') ORDER BY tuple()
            """
            create_table(
                name=name + "_source", statement=sql, on_cluster="sharded_cluster"
            )

        with And("a distributed table"):
            sql = (
                "CREATE TABLE {name} AS "
                + name
                + "_source"
                + " ENGINE = Distributed(sharded_cluster, default, "
                + f"{name + '_source'}, unique1 % toUInt8(getMacro('shard')))"
            )
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            datafile = os.path.join(current_dir(), "tenk.data")

            with open(datafile, "r") as file:
                lines = file.readlines()

            chunks = [lines[i : i + 1000] for i in range(0, len(lines), 1000)]

            for chunk in chunks:
                with tempfile.NamedTemporaryFile() as file:
                    file.write("".join(chunk).encode("utf-8"))
                    file.flush()
                    self.context.cluster.command(
                        None,
                        f'cat "{file.name}" | {self.context.node.cluster.docker_compose} exec -T {self.context.node.name} clickhouse client -q "INSERT INTO {table} FORMAT TSV"',
                        exitcode=0,
                    )

    return table


@TestStep(Given)
def empsalary_table(self, name="empsalary", distributed=False):
    """Create employee salary reference table."""
    table = None

    data = [
        "('develop', 10, 5200, '2007-08-01')",
        "('sales', 1, 5000, '2006-10-01')",
        "('personnel', 5, 3500, '2007-12-10')",
        "('sales', 4, 4800, '2007-08-08')",
        "('personnel', 2, 3900, '2006-12-23')",
        "('develop', 7, 4200, '2008-01-01')",
        "('develop', 9, 4500, '2008-01-01')",
        "('sales', 3, 4800, '2007-08-01')",
        "('develop', 8, 6000, '2006-10-01')",
        "('develop', 11, 5200, '2007-08-15')",
    ]

    if not distributed:
        with By("creating a table"):
            sql = """
            CREATE TABLE {name} (
                depname LowCardinality(String),
                empno  UInt64,
                salary Int32,
                enroll_date Date
                )
            ENGINE = MergeTree() ORDER BY enroll_date
            """
            table = create_table(name=name, statement=sql)

        with And("populating table with data"):
            sql = f"INSERT INTO {name} VALUES {','.join(data)}"
            self.context.node.query(sql)

    else:
        with By("creating replicated source tables"):
            sql = """
                CREATE TABLE {name} ON CLUSTER sharded_cluster (
                    depname LowCardinality(String),
                    empno  UInt64,
                    salary Int32,
                    enroll_date Date
                    )
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') ORDER BY enroll_date
                """
            create_table(
                name=name + "_source", statement=sql, on_cluster="sharded_cluster"
            )

        with And("a distributed table"):
            sql = (
                "CREATE TABLE {name} AS "
                + name
                + "_source"
                + " ENGINE = Distributed(sharded_cluster, default, "
                + f"{name + '_source'}, empno % toUInt8(getMacro('shard')))"
            )
            table = create_table(name=name, statement=sql)

        with And("populating distributed table with data"):
            with By(
                "inserting one data row at a time",
                description="so that data is sharded between nodes",
            ):
                for row in data:
                    self.context.node.query(
                        f"INSERT INTO {table} VALUES {row}",
                        settings=[("insert_distributed_sync", "1")],
                    )

    with And("dumping all the data in the table"):
        self.context.node.query(f"SELECT * FROM {table}")

    return table


@TestStep(Given)
def create_table(self, name, statement, on_cluster=False):
    """Create table."""
    node = current().context.node
    try:
        with Given(f"I have a {name} table"):
            if on_cluster:
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster}")
            else:
                node.query(f"DROP TABLE IF EXISTS {name}")
            node.query(statement.format(name=name))
        yield name
    finally:
        with Finally("I drop the table"):
            if on_cluster:
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster}")
            else:
                node.query(f"DROP TABLE IF EXISTS {name}")
