import textwrap
from contextlib import contextmanager

from testflows.core import *
from testflows.asserts import error

from aes_encryption.requirements import *
from aes_encryption.tests.common import mysql_modes, hex


@contextmanager
def table(name, node, mysql_node, secret_type):
    """Create a table that can be accessed using MySQL database engine."""
    try:
        with Given("table in MySQL"):
            sql = f"""
                CREATE TABLE {name}(
                    id INT NOT NULL AUTO_INCREMENT,
                    date DATE,
                    name VARCHAR(100),
                    secret {secret_type},
                    PRIMARY KEY ( id )
                );
                """
            with When("I drop the table if exists"):
                mysql_node.command(
                    f'MYSQL_PWD=password mysql -D db -u user -e "DROP TABLE IF EXISTS {name};"',
                    exitcode=0,
                )
            with And("I create a table"):
                mysql_node.command(
                    f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF",
                    exitcode=0,
                )

        with And("I create a database using MySQL database engine"):
            sql = f"""
                CREATE DATABASE mysql_db
                ENGINE = MySQL('{mysql_node.name}:3306', 'db', 'user', 'password')
                """
            with When("I drop database if exists"):
                node.query(f"DROP DATABASE IF EXISTS mysql_db")
            with And("I create database"):
                node.query(textwrap.dedent(sql))
        yield

    finally:
        with And("I drop the database that is using MySQL database engine", flags=TE):
            node.query(f"DROP DATABASE IF EXISTS mysql_db")

        with And("I drop a table in MySQL", flags=TE):
            mysql_node.command(
                f'MYSQL_PWD=password mysql -D db -u user -e "DROP TABLE IF EXISTS {name};"',
                exitcode=0,
            )


@TestOutline(Scenario)
@Examples(
    "mysql_datatype",
    [
        ("VARBINARY(100)",),
        # ("VARCHAR(100)",),
        ("BLOB",),
        # ("TEXT",)
    ],
)
def decrypt(self, mysql_datatype):
    """Check that when using a table provided by MySQL database engine that
    contains a column encrypted in MySQL stored using specified data type
    I can decrypt data in the column using the `decrypt` and `aes_decrypt_mysql`
    functions in the select query.
    """
    node = self.context.node
    mysql_node = self.context.mysql_node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    for func in ["decrypt", "aes_decrypt_mysql"]:
        for mode, key_len, iv_len in mysql_modes:
            exact_key_size = int(mode.split("-")[1]) // 8

            if "ecb" not in mode and not iv_len:
                continue
            if func == "decrypt":
                if iv_len and iv_len != 16:
                    continue
                if key_len != exact_key_size:
                    continue

            with Example(
                f"""{func} mode={mode.strip("'")} key={key_len} iv={iv_len}"""
            ):
                with table("user_data", node, mysql_node, mysql_datatype):
                    example_mode = mode
                    example_key = f"'{key[:key_len]}'"
                    example_iv = None if not iv_len else f"'{iv[:iv_len]}'"

                    with When("I insert encrypted data in MySQL"):
                        sql = f"""
                        SET block_encryption_mode = {example_mode};
                        INSERT INTO user_data VALUES (NULL, '2020-01-01', 'user0', AES_ENCRYPT('secret', {example_key}{(", " + example_iv) if example_iv else ", ''"}));
                        """
                        mysql_node.command(
                            f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF",
                            exitcode=0,
                        )

                    with And("I read encrypted data in MySQL to make sure it is valid"):
                        sql = f"""
                        SET block_encryption_mode = {example_mode};
                        SELECT id, date, name, AES_DECRYPT(secret, {example_key}{(", " + example_iv) if example_iv else ", ''"}) AS secret FROM user_data;
                        """
                        mysql_node.command(
                            f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF",
                            exitcode=0,
                        )

                    with And("I read raw encrypted data in MySQL"):
                        mysql_node.command(
                            f'MYSQL_PWD=password mysql -D db -u user -e "SELECT id, date, name, hex(secret) as secret FROM user_data;"',
                            exitcode=0,
                        )

                    with And("I read raw data using MySQL database engine"):
                        output = node.query(
                            "SELECT id, date, name, hex(secret) AS secret FROM mysql_db.user_data"
                        )

                    with And("I read decrypted data using MySQL database engine"):
                        output = node.query(
                            f"""SELECT hex({func}({example_mode}, secret, {example_key}{(", " + example_iv) if example_iv else ""})) FROM mysql_db.user_data"""
                        ).output.strip()

                    with Then("output should match the original plain text"):
                        assert output == hex("secret"), error()


@TestOutline(Scenario)
@Examples(
    "mysql_datatype",
    [
        ("VARBINARY(100)",),
        # ("VARCHAR(100)",),
        ("BLOB",),
        # ("TEXT",)
    ],
)
def encrypt(self, mysql_datatype):
    """Check that when using a table provided by MySQL database engine that
    we can encrypt data during insert using the `aes_encrypt_mysql` function
    and decrypt it in MySQL.
    """
    node = self.context.node
    mysql_node = self.context.mysql_node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    for func in ["encrypt", "aes_encrypt_mysql"]:
        for mode, key_len, iv_len in mysql_modes:
            exact_key_size = int(mode.split("-")[1]) // 8

            if "ecb" not in mode and not iv_len:
                continue
            if func == "encrypt":
                if iv_len and iv_len != 16:
                    continue
                if key_len != exact_key_size:
                    continue

            with Example(
                f"""{func} mode={mode.strip("'")} key={key_len} iv={iv_len}"""
            ):
                with table("user_data", node, mysql_node, mysql_datatype):
                    example_mode = mode
                    example_key = f"'{key[:key_len]}'"
                    example_iv = None if not iv_len else f"'{iv[:iv_len]}'"
                    example_transform = f"{func}({mode}, secret, {example_key}{(', ' + example_iv) if example_iv else ''})"

                    with When(
                        "I insert encrypted data into a table provided by MySQL database engine"
                    ):
                        node.query(
                            textwrap.dedent(
                                f"""
                            INSERT INTO
                                mysql_db.user_data
                            SELECT
                                id, date, name, {example_transform}
                            FROM
                                input('id Int32, date Date, name String, secret String')
                            FORMAT Values (1, '2020-01-01', 'user0', 'secret')
                            """
                            )
                        )

                    with And("I read decrypted data using MySQL database engine"):
                        output = node.query(
                            f"""SELECT hex(aes_decrypt_mysql({example_mode}, secret, {example_key}{(", " + example_iv) if example_iv else ""})) FROM mysql_db.user_data"""
                        ).output.strip()

                    with Then(
                        "decrypted data from MySQL database engine should should match the original plain text"
                    ):
                        assert output == hex("secret"), error()

                    with And(
                        "I read raw data using MySQL database engine to get expected raw data"
                    ):
                        expected_raw_data = node.query(
                            "SELECT hex(secret) AS secret FROM mysql_db.user_data"
                        ).output.strip()

                    with And("I read raw encrypted data in MySQL"):
                        output = mysql_node.command(
                            f'MYSQL_PWD=password mysql -D db -u user -e "SELECT hex(secret) as secret FROM user_data;"',
                            exitcode=0,
                        ).output.strip()

                    with Then(
                        "check that raw encryted data in MySQL matches the expected"
                    ):
                        assert expected_raw_data in output, error()

                    with And("I decrypt data in MySQL to make sure it is valid"):
                        sql = f"""
                        SET block_encryption_mode = {example_mode};
                        SELECT id, date, name, hex(AES_DECRYPT(secret, {example_key}{(", " + example_iv) if example_iv else ", ''"})) AS secret FROM user_data;
                        """
                        output = mysql_node.command(
                            f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF",
                            exitcode=0,
                        ).output.strip()

                    with Then(
                        "decryted data in MySQL should match the original plain text"
                    ):
                        assert hex("secret") in output, error()


@TestFeature
@Name("database engine")
@Requirements(RQ_SRS008_AES_Functions_Compatibility_Engine_Database_MySQL("1.0"))
def feature(self, node="clickhouse1", mysql_node="mysql1"):
    """Check usage of encryption functions with [MySQL database engine].

    [MySQL database engine]: https://clickhouse.com/docs/en/engines/database-engines/mysql/
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
