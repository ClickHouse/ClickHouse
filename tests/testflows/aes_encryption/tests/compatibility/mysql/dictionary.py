import textwrap
from contextlib import contextmanager

from testflows.core import *
from testflows.asserts import error

from aes_encryption.requirements import *
from aes_encryption.tests.common import mysql_modes, hex

@contextmanager
def dictionary(name, node, mysql_node, secret_type):
    """Create a table in MySQL and use it a source for a dictionary.
    """
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
                    id Int32,
                    date Date,
                    name String,
                    secret String
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
def parameters_dictionary(name, node, mysql_node):
    """Create a table in MySQL and use it a source for a dictionary
    that stores parameters for the encryption functions.
    """
    try:
        with Given("table in MySQL"):
            sql = f"""
                CREATE TABLE {name}(
                    `id` INT NOT NULL AUTO_INCREMENT,
                    `name` VARCHAR(100),
                    `mode` VARCHAR(100),
                    `key` BLOB,
                    `iv` BLOB,
                    `text` BLOB,
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
                    id Int32,
                    name String,
                    mode String,
                    key String,
                    iv String,
                    text String
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

@TestScenario
def parameter_values(self):
    """Check that we can use a dictionary that uses MySQL table as a source
    can be used as a parameters store for the `encrypt`, `decrypt`, and
    `aes_encrypt_mysql`, `aes_decrypt_mysql` functions.
    """
    node = self.context.node
    mysql_node = self.context.mysql_node
    mode = "'aes-128-cbc'"
    key = f"'{'1' * 16}'"
    iv = f"'{'2' * 16}'"
    plaintext = "'secret'"

    for encrypt, decrypt in [
            ("encrypt", "decrypt"),
            ("aes_encrypt_mysql", "aes_decrypt_mysql")
        ]:
        with Example(f"{encrypt} and {decrypt}", description=f"Check using dictionary for parameters of {encrypt} and {decrypt} functions."):
            with parameters_dictionary("parameters_data", node, mysql_node) as dict_name:
                with When("I insert parameters values in MySQL"):
                    sql = f"""
                        INSERT INTO parameters_data VALUES (1, 'user0', {mode}, {key}, {iv}, {plaintext});
                    """
                    mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

                with And("I use dictionary values as parameters"):
                    sql = f"""
                        SELECT {decrypt}(
                            dictGet('default.{dict_name}', 'mode', toUInt64(1)),
                            {encrypt}(
                                dictGet('default.{dict_name}', 'mode', toUInt64(1)),
                                dictGet('default.{dict_name}', 'text', toUInt64(1)),
                                dictGet('default.{dict_name}', 'key', toUInt64(1)),
                                dictGet('default.{dict_name}', 'iv', toUInt64(1))
                            ),
                            dictGet('default.{dict_name}', 'key', toUInt64(1)),
                            dictGet('default.{dict_name}', 'iv', toUInt64(1))
                        )
                        """
                    output = node.query(textwrap.dedent(sql)).output.strip()

                with Then("output should match the plain text"):
                    assert f"'{output}'" == plaintext, error()

@TestOutline(Scenario)
@Examples("mysql_datatype", [
    ("VARBINARY(100)",),
    #("VARCHAR(100)",),
    ("BLOB", ),
    #("TEXT",)
])
def decrypt(self, mysql_datatype):
    """Check that when using a dictionary that uses MySQL table as a source and
    contains a data encrypted in MySQL and stored using specified data type
    that we can decrypt data from the dictionary using
    the `aes_decrypt_mysql` or `decrypt` functions in the select query.
    """
    node = self.context.node
    mysql_node = self.context.mysql_node
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    for func in ["decrypt", "aes_decrypt_mysql"]:
        for mode, key_len, iv_len in mysql_modes:
            exact_key_size = int(mode.split("-")[1])//8

            if "ecb" not in mode and not iv_len:
                continue
            if func == "decrypt":
                if iv_len and iv_len != 16:
                    continue
                if key_len != exact_key_size:
                    continue

            with Example(f"""{func} mode={mode.strip("'")} key={key_len} iv={iv_len}"""):
                with dictionary("user_data", node, mysql_node, mysql_datatype) as dict_name:
                    example_mode = mode
                    example_key = f"'{key[:key_len]}'"
                    example_iv = None if not iv_len else f"'{iv[:iv_len]}'"

                    with When("I insert encrypted data in MySQL"):
                        sql = f"""
                        SET block_encryption_mode = {example_mode};
                        INSERT INTO user_data VALUES (NULL, '2020-01-01', 'user0', AES_ENCRYPT('secret', {example_key}{(", " + example_iv) if example_iv else ", ''"}));
                        """
                        mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

                    with And("I read encrypted data in MySQL to make sure it is valid"):
                        sql = f"""
                        SET block_encryption_mode = {example_mode};
                        SELECT id, date, name, AES_DECRYPT(secret, {example_key}{(", " + example_iv) if example_iv else ", ''"}) AS secret FROM user_data;
                        """
                        mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user <<'EOF'{textwrap.dedent(sql)}\nEOF", exitcode=0)

                    with And("I read raw encrypted data in MySQL"):
                        mysql_node.command(f"MYSQL_PWD=password mysql -D db -u user -e \"SELECT id, date, name, hex(secret) as secret FROM user_data;\"", exitcode=0)

                    with And("I read raw data using MySQL dictionary"):
                        output = node.query(f"SELECT hex(dictGet('default.{dict_name}', 'secret', toUInt64(1))) AS secret")

                    with And("I read decrypted data using MySQL dictionary"):
                        output = node.query(textwrap.dedent(f"""
                            SELECT hex(
                                {func}(
                                    {example_mode},
                                    dictGet('default.{dict_name}', 'secret', toUInt64(1)),
                                    {example_key}{(", " + example_iv) if example_iv else ""}
                                )
                            )
                            """)).output.strip()

                    with Then("output should match the original plain text"):
                        assert output == hex("secret"), error()

@TestFeature
@Name("dictionary")
@Requirements(
    RQ_SRS008_AES_Functions_Compatability_Dictionaries("1.0")
)
def feature(self, node="clickhouse1", mysql_node="mysql1"):
    """Check usage of encryption functions with [MySQL dictionary].

    [MySQL dictionary]: https://clickhouse.tech/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources/#dicts-external_dicts_dict_sources-mysql
    """
    self.context.node = self.context.cluster.node(node)
    self.context.mysql_node = self.context.cluster.node(mysql_node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
