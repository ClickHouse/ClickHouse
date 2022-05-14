import os
import textwrap
from contextlib import contextmanager
from importlib.machinery import SourceFileLoader

from testflows.core import *
from testflows.core.name import basename
from testflows.asserts.helpers import varname
from testflows.asserts import values, error, snapshot

from aes_encryption.tests.common import modes, mysql_modes


@contextmanager
def table(name):
    node = current().context.node
    try:
        with Given("table"):
            sql = f"""
                CREATE TABLE {name}
                (
                    date Nullable(Date),
                    name Nullable(String),
                    secret Nullable(String)
                )
                ENGINE = Memory()
                """
            with By("dropping table if exists"):
                node.query(f"DROP TABLE IF EXISTS {name}")
            with And("creating a table"):
                node.query(textwrap.dedent(sql))
        yield
    finally:
        with Finally("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {name}")


@contextmanager
def mv_transform(table, transform):
    node = current().context.node
    try:
        with Given("tables for input transformation"):
            with By("creating Null input table"):
                sql = f"""
                    CREATE TABLE {table}_input
                    (
                        date Nullable(Date),
                        name Nullable(String),
                        secret Nullable(String),
                        mode String,
                        key String,
                        iv String,
                        aad String
                    )
                    ENGINE=Null()
                    """
                node.query(textwrap.dedent(sql))

            with And("creating materialized view table"):
                sql = f"""
                    CREATE MATERIALIZED VIEW {table}_input_mv TO {table} AS
                    SELECT date, name, {transform}
                    FROM {table}_input
                    """
                node.query(textwrap.dedent(sql))
        yield
    finally:
        with Finally("I drop tables for input transformation", flags=TE):
            with By("dropping materialized view table", flags=TE):
                node.query(f"DROP TABLE IF EXISTS {table}_input_mv")

            with And("dropping Null input table", flags=TE):
                node.query(f"DROP TABLE IF EXISTS {table}_input")


@TestScenario
def encrypt_using_materialized_view(self):
    """Check that we can use `encrypt` function when inserting
    data into a table using a materialized view for input
    data transformation.
    """
    node = self.context.node
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_aad = None if not aad_len else f"'{aad}'"
            example_transform = f"encrypt(mode, secret, key{', iv' if example_iv else ''}{', aad' if example_aad else ''})"

            with table("user_data"):
                with mv_transform("user_data", example_transform):
                    with When("I insert encrypted data"):
                        node.query(
                            f"""
                            INSERT INTO user_data_input
                                (date, name, secret, mode, key)
                            VALUES
                                ('2020-01-01', 'user0', 'user0_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}),
                                ('2020-01-02', 'user1', 'user1_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}),
                                ('2020-01-03', 'user2', 'user2_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""})
                            """
                        )

                    with And("I read inserted data back"):
                        node.query(
                            "SELECT date, name, hex(secret) FROM user_data ORDER BY date"
                        )

                    with Then("output must match the snapshot"):
                        with values() as that:
                            assert that(
                                snapshot(
                                    r.output.strip(),
                                    "insert",
                                    name=f"encrypt_mv_example_{varname(basename(self.name))}",
                                )
                            ), error()


@TestScenario
def aes_encrypt_mysql_using_materialized_view(self):
    """Check that we can use `aes_encrypt_mysql` function when inserting
    data into a table using a materialized view for input
    data transformation.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    for mode, key_len, iv_len in mysql_modes:
        with Example(
            f"""mode={mode.strip("'")} key={key_len} iv={iv_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_transform = (
                f"aes_encrypt_mysql(mode, secret, key{', iv' if example_iv else ''})"
            )

            with table("user_data"):
                with mv_transform("user_data", example_transform):
                    with When("I insert encrypted data"):
                        node.query(
                            f"""
                            INSERT INTO user_data_input
                                (date, name, secret, mode, key)
                            VALUES
                                ('2020-01-01', 'user0', 'user0_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}),
                                ('2020-01-02', 'user1', 'user1_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}),
                                ('2020-01-03', 'user2', 'user2_secret', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""})
                            """
                        )

                    with And("I read inserted data back"):
                        node.query(
                            "SELECT date, name, hex(secret) FROM user_data ORDER BY date"
                        )

                    with Then("output must match the snapshot"):
                        with values() as that:
                            assert that(
                                snapshot(
                                    r.output.strip(),
                                    "insert",
                                    name=f"aes_encrypt_mysql_mv_example_{varname(basename(self.name))}",
                                )
                            ), error()


@TestScenario
def encrypt_using_input_table_function(self):
    """Check that we can use `encrypt` function when inserting
    data into a table using insert select and `input()` table
    function.
    """
    node = self.context.node
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_aad = None if not aad_len else f"'{aad}'"
            example_transform = f"encrypt({mode}, secret, {example_key}{(', ' + example_iv) if example_iv else ''}{(', ' + example_aad) if example_aad else ''})"

            with table("user_data"):
                with When("I insert encrypted data"):
                    node.query(
                        f"""
                        INSERT INTO
                            user_data
                        SELECT
                            date, name, {example_transform}
                        FROM
                            input('date Date, name String, secret String')
                        FORMAT Values ('2020-01-01', 'user0', 'user0_secret'), ('2020-01-02', 'user1', 'user1_secret'), ('2020-01-03', 'user2', 'user2_secret')
                        """
                    )

                with And("I read inserted data back"):
                    r = node.query(
                        "SELECT date, name, hex(secret) FROM user_data ORDER BY date"
                    )

                with Then("output must match the snapshot"):
                    with values() as that:
                        assert that(
                            snapshot(
                                r.output.strip(),
                                "insert",
                                name=f"encrypt_input_example_{varname(basename(example.name))}",
                            )
                        ), error()


@TestScenario
def aes_encrypt_mysql_using_input_table_function(self):
    """Check that we can use `aes_encrypt_mysql` function when inserting
    data into a table using insert select and `input()` table
    function.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    for mode, key_len, iv_len in mysql_modes:
        with Example(
            f"""mode={mode.strip("'")} key={key_len} iv={iv_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_transform = f"aes_encrypt_mysql({mode}, secret, {example_key}{(', ' + example_iv) if example_iv else ''})"

            with table("user_data"):
                with When("I insert encrypted data"):
                    node.query(
                        f"""
                        INSERT INTO
                            user_data
                        SELECT
                            date, name, {example_transform}
                        FROM
                            input('date Date, name String, secret String')
                        FORMAT Values ('2020-01-01', 'user0', 'user0_secret'), ('2020-01-02', 'user1', 'user1_secret'), ('2020-01-03', 'user2', 'user2_secret')
                        """
                    )

                with And("I read inserted data back"):
                    r = node.query(
                        "SELECT date, name, hex(secret) FROM user_data ORDER BY date"
                    )

                with Then("output must match the snapshot"):
                    with values() as that:
                        assert that(
                            snapshot(
                                r.output.strip(),
                                "insert",
                                name=f"aes_encrypt_mysql_input_example_{varname(basename(example.name))}",
                            )
                        ), error()


@TestScenario
def decrypt_using_materialized_view(self):
    """Check that we can use `decrypt` function when inserting
    data into a table using a materialized view for input
    data transformation.
    """
    node = self.context.node
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "insert.py.insert.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_aad = None if not aad_len else f"'{aad}'"
            example_transform = f"decrypt(mode, secret, key{', iv' if example_iv else ''}{', aad' if example_aad else ''})"

            with Given("I have ciphertexts"):
                example_name = basename(example.name)
                ciphertexts = getattr(
                    snapshot_module, varname(f"encrypt_mv_example_{example_name}")
                )
                example_ciphertexts = [
                    "'{}'".format(l.split("\t")[-1].strup("'"))
                    for l in ciphertexts.split("\n")
                ]

            with table("user_data"):
                with mv_transform("user_data", example_transform):
                    with When("I insert encrypted data"):
                        node.query(
                            f"""
                            INSERT INTO user_data_input
                                (date, name, secret, mode, key)
                            VALUES
                                ('2020-01-01', 'user0', 'unhex({example_ciphertexts[0]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}),
                                ('2020-01-02', 'user1', 'unhex({example_ciphertexts[1]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}),
                                ('2020-01-03', 'user2', 'unhex({example_ciphertexts[2]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""})
                            """
                        )

                    with And("I read inserted data back"):
                        r = node.query(
                            "SELECT date, name, secret FROM user_data ORDER BY date"
                        )

                    with Then("output must match the expected"):
                        expected = r"""'2020-01-01\tuser0\tuser0_secret\n2020-01-02\tuser1\tuser1_secret\n2020-01-03\tuser2\tuser2_secret'"""
                        assert r.output == expected, error()


@TestScenario
def aes_decrypt_mysql_using_materialized_view(self):
    """Check that we can use `aes_decrypt_mysql` function when inserting
    data into a table using a materialized view for input
    data transformation.
    """
    node = self.context.node
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "insert.py.insert.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} key={key_len} iv={iv_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_aad = None if not aad_len else f"'{aad}'"
            example_transform = (
                f"aes_decrypt_mysql(mode, secret, key{', iv' if example_iv else ''})"
            )

            with Given("I have ciphertexts"):
                example_name = basename(example.name)
                ciphertexts = getattr(
                    snapshot_module,
                    varname(f"aes_encrypt_mysql_mv_example_{example_name}"),
                )
                example_ciphertexts = [
                    "'{}'".format(l.split("\t")[-1].strup("'"))
                    for l in ciphertexts.split("\n")
                ]

            with table("user_data"):
                with mv_transform("user_data", example_transform):
                    with When("I insert encrypted data"):
                        node.query(
                            f"""
                            INSERT INTO user_data_input
                                (date, name, secret, mode, key)
                            VALUES
                                ('2020-01-01', 'user0', 'unhex({example_ciphertexts[0]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}),
                                ('2020-01-02', 'user1', 'unhex({example_ciphertexts[1]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""}),
                                ('2020-01-03', 'user2', 'unhex({example_ciphertexts[2]})', {example_mode}, {example_key}{(", " + example_iv) if example_iv else ""})
                            """
                        )

                    with And("I read inserted data back"):
                        r = node.query(
                            "SELECT date, name, secret FROM user_data ORDER BY date"
                        )

                    with Then("output must match the expected"):
                        expected = r"""'2020-01-01\tuser0\tuser0_secret\n2020-01-02\tuser1\tuser1_secret\n2020-01-03\tuser2\tuser2_secret'"""
                        assert r.output == expected, error()


@TestScenario
def decrypt_using_input_table_function(self):
    """Check that we can use `decrypt` function when inserting
    data into a table using insert select and `input()` table
    function.
    """
    node = self.context.node
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "insert.py.insert.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_aad = None if not aad_len else f"'{aad}'"
            example_transform = f"decrypt({mode}, unhex(secret), {example_key}{(', ' + example_iv) if example_iv else ''}{(', ' + example_aad) if example_aad else ''})"

            with Given("I have ciphertexts"):
                example_name = basename(example.name)
                ciphertexts = getattr(
                    snapshot_module, varname(f"encrypt_input_example_{example_name}")
                )
                example_ciphertexts = [
                    l.split("\\t")[-1].strip("'") for l in ciphertexts.split("\\n")
                ]

            with table("user_data"):
                with When("I insert decrypted data"):
                    node.query(
                        textwrap.dedent(
                            f"""
                        INSERT INTO
                            user_data
                        SELECT
                            date, name, {example_transform}
                        FROM
                            input('date Date, name String, secret String')
                        FORMAT Values ('2020-01-01', 'user0', '{example_ciphertexts[0]}'), ('2020-01-02', 'user1', '{example_ciphertexts[1]}'), ('2020-01-03', 'user2', '{example_ciphertexts[2]}')
                        """
                        )
                    )

                with And("I read inserted data back"):
                    r = node.query(
                        "SELECT date, name, secret FROM user_data ORDER BY date"
                    )

                expected = """2020-01-01\tuser0\tuser0_secret\n2020-01-02\tuser1\tuser1_secret\n2020-01-03\tuser2\tuser2_secret"""
                with Then("output must match the expected", description=expected):
                    assert r.output == expected, error()


@TestScenario
def aes_decrypt_mysql_using_input_table_function(self):
    """Check that we can use `aes_decrypt_mysql` function when inserting
    data into a table using insert select and `input()` table
    function.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "insert.py.insert.snapshot"),
        ).load_module()

    for mode, key_len, iv_len in mysql_modes:
        with Example(
            f"""mode={mode.strip("'")} key={key_len} iv={iv_len}"""
        ) as example:
            example_key = f"'{key[:key_len]}'"
            example_mode = mode
            example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            example_transform = f"aes_decrypt_mysql({mode}, unhex(secret), {example_key}{(', ' + example_iv) if example_iv else ''})"

            with Given("I have ciphertexts"):
                example_name = basename(example.name)
                ciphertexts = getattr(
                    snapshot_module,
                    varname(f"aes_encrypt_mysql_input_example_{example_name}"),
                )
                example_ciphertexts = [
                    l.split("\\t")[-1].strip("'") for l in ciphertexts.split("\\n")
                ]

            with table("user_data"):
                with When("I insert decrypted data"):
                    node.query(
                        textwrap.dedent(
                            f"""
                        INSERT INTO
                            user_data
                        SELECT
                            date, name, {example_transform}
                        FROM
                            input('date Date, name String, secret String')
                        FORMAT Values ('2020-01-01', 'user0', '{example_ciphertexts[0]}'), ('2020-01-02', 'user1', '{example_ciphertexts[1]}'), ('2020-01-03', 'user2', '{example_ciphertexts[2]}')
                        """
                        )
                    )

                with And("I read inserted data back"):
                    r = node.query(
                        "SELECT date, name, secret FROM user_data ORDER BY date"
                    )

                expected = """2020-01-01\tuser0\tuser0_secret\n2020-01-02\tuser1\tuser1_secret\n2020-01-03\tuser2\tuser2_secret"""
                with Then("output must match the expected", description=expected):
                    assert r.output == expected, error()


@TestFeature
@Name("insert")
def feature(self, node="clickhouse1"):
    """Check encryption functions when used during data insertion into a table."""
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
