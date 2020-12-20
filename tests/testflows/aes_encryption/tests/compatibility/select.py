import textwrap
from contextlib import contextmanager

from testflows.core import *
from testflows.asserts.helpers import varname
from testflows.asserts import values, error, snapshot

from aes_encryption.tests.common import modes, mysql_modes

@contextmanager
def table(name, sql):
    node = current().context.node
    try:
        with Given("table"):

            with By("dropping table if exists"):
                node.query(f"DROP TABLE IF EXISTS {name}")
            with And("creating a table"):
                node.query(textwrap.dedent(sql.format(name=name)))
        yield
    finally:
        with Finally("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {name}")

@TestScenario
def decrypt(self):
    """Check decrypting column when reading data from a table.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    for mode, key_len, iv_len, aad_len in modes:
        with Example(f"""mode={mode.strip("'")} key={key_len} iv={iv_len} aad={aad_len}""") as example:
            with table("user_table", """
                        CREATE TABLE {name}
                        (
                            date Nullable(Date),
                            name Nullable(String),
                            secret Nullable(String)
                        )
                        ENGINE = Memory()
                        """):

                example_mode = mode
                example_key = f"'{key[:key_len]}'"
                example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
                example_aad = None if not aad_len else f"'{aad}'"

                with When("I insert encrypted data"):
                    encrypted_secret = node.query(f"""SELECT hex(encrypt({example_mode}, 'secret', {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}))""").output.strip()
                    node.query(textwrap.dedent(f"""
                        INSERT INTO user_table
                            (date, name, secret)
                        VALUES
                            ('2020-01-01', 'user0', unhex('{encrypted_secret}'))
                        """))

                with And("I decrypt data during query"):
                    output = node.query(f"""SELECT name, decrypt({example_mode}, secret, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}) AS secret FROM user_table FORMAT JSONEachRow""").output.strip()

                with Then("I should get back the original plain text"):
                    assert output == '{"name":"user0","secret":"secret"}', error()

@TestScenario
def decrypt_multiple(self, count=1000):
    """Check decrypting column when reading multiple entries
    encrypted with the same parameters for the same user
    from a table.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    for mode, key_len, iv_len, aad_len in modes:
        with Example(f"""mode={mode.strip("'")} key={key_len} iv={iv_len} aad={aad_len}""") as example:
            with table("user_table", """
                    CREATE TABLE {name}
                    (
                        date Nullable(Date),
                        name Nullable(String),
                        secret Nullable(String)
                    )
                    ENGINE = Memory()
                    """):

                example_mode = mode
                example_key = f"'{key[:key_len]}'"
                example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
                example_aad = None if not aad_len else f"'{aad}'"

                with When("I insert encrypted data"):
                    encrypted_secret = node.query(f"""SELECT hex(encrypt({example_mode}, 'secret', {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}))""").output.strip()
                    values = [f"('2020-01-01', 'user0', unhex('{encrypted_secret}'))"] * count
                    node.query(
                        "INSERT INTO user_table\n"
                        "   (date, name, secret)\n"
                        f"VALUES {', '.join(values)}")

                with And("I decrypt data", description="using a subquery and get the number of entries that match the plaintext"):
                    output = node.query(f"""SELECT count() AS count FROM (SELECT name, decrypt({example_mode}, secret, {example_key}{(", " + example_iv) if example_iv else ""}{(", " + example_aad) if example_aad else ""}) AS secret FROM user_table) WHERE secret = 'secret' FORMAT JSONEachRow""").output.strip()

                with Then("I should get back the expected result", description=f"{count}"):
                    assert output == f'{{"count":"{count}"}}', error()

@TestScenario
def decrypt_unique(self):
    """Check decrypting column when reading multiple entries
    encrypted with the different parameters for each user
    from a table.
    """
    node = self.context.node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    aad = "some random aad"

    with table("user_table", """
            CREATE TABLE {name}
            (
                id UInt64,
                date Nullable(Date),
                name Nullable(String),
                secret Nullable(String)
            )
            ENGINE = Memory()
            """):

        user_modes = []
        user_keys = []

        with When("I get encrypted data"):
            user_id = 0
            values = []

            for mode, key_len, iv_len, aad_len in modes:
                if "gcm" in mode:
                    continue
                user_modes.append(mode)
                user_keys.append(f"'{key[:key_len]}'")

                with When(f"I get encrypted data for user {user_id}"):
                    encrypted_secret = node.query(
                            f"""SELECT hex(encrypt({user_modes[-1]}, 'secret', {user_keys[-1]}))"""
                        ).output.strip()
                    values.append(f"({user_id}, '2020-01-01', 'user{user_id}', unhex('{encrypted_secret}'))")

                user_id += 1

        with And("I insert encrypted data for all users"):
            node.query(
                "INSERT INTO user_table\n"
                "   (id, date, name, secret)\n"
                f"VALUES {', '.join(values)}")

        with And("I read decrypted data for all users"):
            output = node.query(textwrap.dedent(f"""
                SELECT
                    count() AS count
                FROM
                (
                    SELECT
                        [{",".join(user_modes)}] AS modes,
                        [{",".join(user_keys)}] AS keys,
                        name,
                        decrypt(modes[id], secret, keys[id]) AS secret
                    FROM user_table
                )
                WHERE
                    secret = 'secret'
                FORMAT JSONEachRow
                """)).output.strip()

        with Then("I should get back the expected result", description=f"{count}"):
            assert output == f'{{"count":"{count}"}}', error()

@TestFeature
@Name("select")
def feature(self, node="clickhouse1"):
    """Check encryption functions when used during table querying.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
