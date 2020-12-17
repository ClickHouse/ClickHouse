from testflows.core import *
from testflows.core.name import basename
from testflows.asserts import values, error, snapshot

from aes_encryption.requirements.requirements import *
from aes_encryption.tests.common import *

@TestOutline
def aes_encrypt_mysql(self, plaintext=None, key=None, mode=None, iv=None, exitcode=0, message=None, step=When):
    """Execute `aes_encrypt_mysql` function with the specified parameters.
    """
    params = []
    if mode is not None:
        params.append(mode)
    if plaintext is not None:
        params.append(plaintext)
    if key is not None:
        params.append(key)
    if iv is not None:
        params.append(iv)

    sql = "SELECT hex(aes_encrypt_mysql(" + ", ".join(params) + "))"

    return current().context.node.query(sql, step=step, exitcode=exitcode, message=message)

@TestOutline(Scenario)
@Examples("mode", [
    ("'aes-128-gcm'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_GCM_Error("1.0"))),
    ("'aes-192-gcm'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_GCM_Error("1.0"))),
    ("'aes-256-gcm'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_GCM_Error("1.0"))),
    ("'aes-128-ctr'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CTR_Error("1.0"))),
    ("'aes-192-ctr'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CTR_Error("1.0"))),
    ("'aes-256-ctr'", Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CTR_Error("1.0"))),
])
def unsupported_modes(self, mode):
    """Check that `aes_encrypt_mysql` function returns an error when unsupported modes are specified.
    """
    aes_encrypt_mysql(plaintext="'hello there'", mode=mode, key=f"'{'1'* 32}'", exitcode=36, message="DB::Exception: Unsupported cipher mode")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_InvalidParameters("1.0")
)
def invalid_parameters(self):
    """Check that `aes_encrypt_mysql` function returns an error when
    we call it with invalid parameters.
    """
    with Example("no parameters"):
        aes_encrypt_mysql(exitcode=42, message="DB::Exception: Incorrect number of arguments for function aes_encrypt provided 0, expected 3 to 4")

    with Example("missing key and mode"):
        aes_encrypt_mysql(plaintext="'hello there'", exitcode=42, message="DB::Exception: Incorrect number of arguments for function aes_encrypt_mysql provided 1")

    with Example("missing mode"):
        aes_encrypt_mysql(plaintext="'hello there'", key="'123'", exitcode=42, message="DB::Exception: Incorrect number of arguments for function aes_encrypt_mysql provided 2")

    with Example("bad key type - UInt8"):
        aes_encrypt_mysql(plaintext="'hello there'", key="123", mode="'aes-128-ecb'", exitcode=43,
            message="DB::Exception: Received from localhost:9000. DB::Exception: Illegal type of argument #3")

    with Example("bad mode type - forgot quotes"):
        aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="aes-128-ecb", exitcode=47,
            message="DB::Exception: Missing columns: 'ecb' 'aes' while processing query")

    with Example("bad mode type - UInt8"):
        aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="128", exitcode=43,
            message="DB::Exception: Illegal type of argument #1 'mode'")

    with Example("bad iv type - UInt8"):
        aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-cbc'", iv='128', exitcode=43,
            message="DB::Exception: Illegal type of argument")

    with Example("iv not valid for mode", requirements=[RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-ecb'", iv="'012345678912'", exitcode=36,
            message="DB::Exception: aes-128-ecb does not support IV")

    with Example("iv not valid for mode - size 0", requirements=[RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-ecb'", iv="''", exitcode=0,
            message=None)

    with Example("invalid mode value", requirements=[RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_Invalid("1.0")]):
        with When("typo in the block algorithm"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-eeb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-eeb")

        with When("typo in the key size"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-127-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-127-ecb")

        with When("typo in the aes prefix"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aee-128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aee-128-ecb")

        with When("missing last dash"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128ecb")

        with When("missing first dash"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'aes128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes128-ecb")

        with When("all capitals"):
            aes_encrypt_mysql(plaintext="'hello there'", key="'0123456789123456'", mode="'AES-128-ECB'", exitcode=36,
                message="DB::Exception: Invalid mode: AES-128-ECB")

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Key_Length_TooShortError("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_Key_Length_TooLong("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_Length_TooShortError("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_Length_TooLong("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_NotValidForMode("1.0")
)
@Examples("mode key_len iv_len", [
    # ECB
    ("'aes-128-ecb'", 16, None,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_ECB_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-ecb'", 24, None,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_ECB_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-ecb'", 32, None,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_ECB_KeyAndInitializationVector_Length("1.0"))),
    # CBC
    ("'aes-128-cbc'", 16, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CBC_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-cbc'", 24, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CBC_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-cbc'", 32, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CBC_KeyAndInitializationVector_Length("1.0"))),
    # CFB1
    ("'aes-128-cfb1'", 16, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB1_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-cfb1'", 24, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB1_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-cfb1'", 32, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB1_KeyAndInitializationVector_Length("1.0"))),
    # CFB8
    ("'aes-128-cfb8'", 16, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB8_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-cfb8'", 24, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB8_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-cfb8'", 32, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB8_KeyAndInitializationVector_Length("1.0"))),
    # CFB128
    ("'aes-128-cfb128'", 16, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB128_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-cfb128'", 24, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB128_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-cfb128'", 32, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB128_KeyAndInitializationVector_Length("1.0"))),
    # OFB
    ("'aes-128-ofb'", 16, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_OFB_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-192-ofb'", 24, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_OFB_KeyAndInitializationVector_Length("1.0"))),
    ("'aes-256-ofb'", 32, 16,
     Requirements(RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_OFB_KeyAndInitializationVector_Length("1.0"))),
], "%-16s %-10s %-10s")
def key_or_iv_length_for_mode(self, mode, key_len, iv_len):
    """Check that key or iv length for mode.
    """
    plaintext = "'hello there'"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    with When("key is too long"):
        aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len+1]}'", mode=mode)

    if iv_len is not None:
        with When("iv is too short"):
            aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

        with When("iv is too long"):
            aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len+1]}'", mode=mode)
    else:
        with When("iv is specified but not needed"):
            aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_InitializationVector("1.0")
)
def iv_parameter_types(self):
    """Check that `aes_encrypt_mysql` function accepts `iv` parameter as the fourth argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("iv is specified using String type"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, iv=iv, message="F024F9372FA0D8B974894D29FFB8A7F7")

    with When("iv is specified using String with UTF8 characters"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, iv="'Gãńdåłf_Thê'", message="7A4EC0FF3796F46BED281F4778ACE1DC")

    with When("iv is specified using FixedString type"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, iv=f"toFixedString({iv}, 16)", message="F024F9372FA0D8B974894D29FFB8A7F7")

    with When("iv is specified using FixedString with UTF8 characters"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, iv="toFixedString('Gãńdåłf_Thê', 16)", message="7A4EC0FF3796F46BED281F4778ACE1DC")


@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Key("1.0")
)
def key_parameter_types(self):
    """Check that `aes_encrypt_mysql` function accepts `key` parameter as the second argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("key is specified using String type"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("key is specified using String with UTF8 characters"):
        aes_encrypt_mysql(plaintext=plaintext, key="'Gãńdåłf_Thê'", mode=mode, message="180086AA42AD57B71C706EEC372D0C3D")

    with When("key is specified using FixedString type"):
        aes_encrypt_mysql(plaintext=plaintext, key=f"toFixedString({key}, 16)", mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("key is specified using FixedString with UTF8 characters"):
        aes_encrypt_mysql(plaintext=plaintext, key="toFixedString('Gãńdåłf_Thê', 16)", mode=mode, message="180086AA42AD57B71C706EEC372D0C3D")


@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode("1.0"),
)
def mode_parameter_types(self):
    """Check that `aes_encrypt_mysql` function accepts `mode` parameter as the third argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("mode is specified using String type"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("mode is specified using FixedString type"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=f"toFixedString({mode}, 12)", message="49C9ADB81BA9B58C485E7ADB90E70576")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_ReturnValue("1.0")
)
def return_value(self):
    """Check that `aes_encrypt_mysql` functions returns String data type.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("I get type of the return value"):
        sql = "SELECT toTypeName(aes_encrypt_mysql("+ mode + "," + plaintext + "," + key + "," + iv + "))"
        r = self.context.node.query(sql)

    with Then("type should be String"):
        assert r.output.strip() == "String", error()

    with When("I get return ciphertext as hex"):
        aes_encrypt_mysql(plaintext=plaintext, key=key, mode=mode, iv=iv, message="F024F9372FA0D8B974894D29FFB8A7F7")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Syntax("1.0"),
)
def syntax(self):
    """Check that `aes_encrypt_mysql` function supports syntax

    ```sql
    aes_encrypt_mysql(plaintext, key, mode, [iv])
    ```
    """
    sql = "SELECT hex(aes_encrypt_mysql('aes-128-ofb', 'hello there', '0123456789123456', '0123456789123456'))"
    self.context.node.query(sql, step=When, message="70FE78410D6EE237C2DE4A")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_PlainText("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode("1.0"),
    RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_ValuesFormat("1.0"),
)
def encryption(self):
    """Check that `aes_encrypt_mysql` functions accepts `plaintext` as the second parameter
    with any data type and `mode` as the first parameter.
    """
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    for mode, key_len, iv_len in mysql_modes:
        for datatype, plaintext in plaintexts:
            requirement = globals().get(f"""RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_{mode.strip("'").replace("-","_").upper()}""")("1.0")

            with Example(f"""mode={mode.strip("'")} datatype={datatype.strip("'")} key={key_len} iv={iv_len}""",
                    requirements=[requirement]) as example:

                r = aes_encrypt_mysql(plaintext=plaintext, key=f"'{key[:key_len]}'", mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"))

                with Then("I check output against snapshot"):
                    with values() as that:
                        example_name = basename(example.name)
                        assert that(snapshot(r.output.strip(), "encrypt_mysql", name=f"example_{example_name.replace(' ', '_')}")), error()

@TestFeature
@Name("encrypt_mysql")
@Requirements(
    RQ_SRS008_AES_MySQL_Encrypt_Function("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the behavior of the `aes_encrypt_mysql` function.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
