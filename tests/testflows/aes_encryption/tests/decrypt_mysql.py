# -*- coding: utf-8 -*-
import os
from importlib.machinery import SourceFileLoader

from testflows.core import *
from testflows.core.name import basename
from testflows.asserts.helpers import varname
from testflows.asserts import error

from aes_encryption.requirements.requirements import *
from aes_encryption.tests.common import *

@TestOutline
def aes_decrypt_mysql(self, ciphertext=None, key=None, mode=None, iv=None, aad=None, exitcode=0, message=None,
        step=When, cast=None, endcast=None, compare=None, no_checks=False):
    """Execute `aes_decrypt_mysql` function with the specified parameters.
    """
    params = []
    if mode is not None:
        params.append(mode)
    if ciphertext is not None:
        params.append(ciphertext)
    if key is not None:
        params.append(key)
    if iv is not None:
        params.append(iv)
    if aad is not None:
        params.append(aad)

    sql = f"aes_decrypt_mysql(" + ", ".join(params) + ")"
    if cast:
        sql = f"{cast}({sql}){endcast or ''}"
    if compare:
        sql = f"{compare} = {sql}"
    sql = f"SELECT {sql}"

    return current().context.node.query(sql, step=step, exitcode=exitcode, message=message, no_checks=no_checks)

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_CipherText("1.0"),
)
def invalid_ciphertext(self):
    """Check that `aes_decrypt_mysql` function does not crash when invalid
    `ciphertext` is passed as the first parameter.
    """
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    invalid_ciphertexts = plaintexts

    for mode, key_len, iv_len in mysql_modes:
        with Example(f"""mode={mode.strip("'")} iv={iv_len}"""):
            d_iv = None if not iv_len else f"'{iv[:iv_len]}'"

            for datatype, ciphertext in invalid_ciphertexts:
                if datatype == "NULL" or datatype.endswith("Null"):
                    continue
                with When(f"invalid ciphertext={ciphertext}"):
                    if "cfb" in mode or "ofb" in mode or "ctr" in mode:
                        aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", mode=mode, iv=d_iv, cast="hex")
                    else:
                        with When("I execute aes_decrypt_mysql function"):
                            r = aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", mode=mode, iv=d_iv, no_checks=True, step=By)
                        with Then("exitcode is not zero"):
                            assert r.exitcode in [198, 36]
                        with And("exception is present in the output"):
                            assert "DB::Exception:" in r.output

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Values_GCM_Error("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Values_CTR_Error("1.0")
)
@Examples("mode", [
    ("'aes-128-gcm'",),
    ("'aes-192-gcm'",),
    ("'aes-256-gcm'",),
    ("'aes-128-ctr'",),
    ("'aes-192-ctr'",),
    ("'aes-256-ctr'",)
])
def unsupported_modes(self, mode):
    """Check that `aes_decrypt_mysql` function returns an error when unsupported modes are specified.
    """
    ciphertext = "unhex('AA1826B5F66A903C888D5DCDA9FB63D1D9CCA10EC55F59D6C00D37')"

    aes_decrypt_mysql(ciphertext=ciphertext, mode=mode, key=f"'{'1'* 32}'", exitcode=36, message="DB::Exception: Unsupported cipher mode")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_InvalidParameters("1.0")
)
def invalid_parameters(self):
    """Check that `aes_decrypt_mysql` function returns an error when
    we call it with invalid parameters.
    """
    ciphertext = "unhex('AA1826B5F66A903C888D5DCDA9FB63D1D9CCA10EC55F59D6C00D37')"

    with Example("no parameters"):
        aes_decrypt_mysql(exitcode=42, message="DB::Exception: Incorrect number of arguments for function aes_decrypt_mysql provided 0, expected 3 to 4")

    with Example("missing key and mode"):
        aes_decrypt_mysql(ciphertext=ciphertext, exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function aes_decrypt_mysql provided 1")

    with Example("missing mode"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'123'", exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function aes_decrypt_mysql provided 2")

    with Example("bad key type - UInt8"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="123", mode="'aes-128-ecb'", exitcode=43,
            message="DB::Exception: Received from localhost:9000. DB::Exception: Illegal type of argument #3")

    with Example("bad mode type - forgot quotes"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="aes-128-ecb", exitcode=47,
            message="DB::Exception: Missing columns: 'ecb' 'aes' while processing query")

    with Example("bad mode type - UInt8"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="128", exitcode=43,
            message="DB::Exception: Illegal type of argument #1 'mode'")

    with Example("bad iv type - UInt8"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-128-cbc'", iv='128', exitcode=43,
            message="DB::Exception: Illegal type of argument")

    with Example("iv not valid for mode", requirements=[RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key="'0123456789123456'", mode="'aes-128-ecb'", iv="'012345678912'", exitcode=0,
            message=None)

    with Example("iv not valid for mode - size 0", requirements=[RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key="'0123456789123456'", mode="'aes-128-ecb'", iv="''", exitcode=0,
            message=None)

    with Example("aad passed by mistake"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-128-cbc'", iv="'0123456789123456'", aad="'aad'", exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function aes_decrypt_mysql provided 5")

    with Example("aad passed by mistake type - UInt8"):
        aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-128-gcm'", iv="'012345678912'", aad="123", exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function aes_decrypt_mysql provided 5")

    with Example("invalid mode value", requirements=[RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_Invalid("1.0")]):
        with When("typo in the block algorithm"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-128-eeb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-eeb")

        with When("typo in the key size"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-127-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-127-ecb")

        with When("typo in the aes prefix"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aee-128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aee-128-ecb")

        with When("missing last dash"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes-128ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128ecb")

        with When("missing first dash"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'aes128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes128-ecb")

        with When("all capitals"):
            aes_decrypt_mysql(ciphertext=ciphertext, key="'0123456789123456'", mode="'AES-128-ECB'", exitcode=36,
                message="DB::Exception: Invalid mode: AES-128-ECB")

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Key_Length_TooShortError("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Key_Length_TooLong("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_Length_TooShortError("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_Length_TooLong("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_NotValidForMode("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Mode_KeyAndInitializationVector_Length("1.0")
)
@Examples("mode key_len iv_len", [
    # ECB
    ("'aes-128-ecb'", 16, None),
    ("'aes-192-ecb'", 24, None),
    ("'aes-256-ecb'", 32, None),
    # CBC
    ("'aes-128-cbc'", 16, 16),
    ("'aes-192-cbc'", 24, 16),
    ("'aes-256-cbc'", 32, 16),
    # CFB128
    ("'aes-128-cfb128'", 16, 16),
    ("'aes-192-cfb128'", 24, 16),
    ("'aes-256-cfb128'", 32, 16),
    # OFB
    ("'aes-128-ofb'", 16, 16),
    ("'aes-192-ofb'", 24, 16),
    ("'aes-256-ofb'", 32, 16)
], "%-16s %-10s %-10s")
def key_or_iv_length_for_mode(self, mode, key_len, iv_len):
    """Check that key or iv length for mode.
    """
    ciphertext = "unhex('31F4C847CAB873AB34584368E3E85E3A')"
    if mode == "'aes-128-ecb'":
        ciphertext = "unhex('31F4C847CAB873AB34584368E3E85E3B')"
    elif mode == "'aes-192-ecb'":
        ciphertext = "unhex('073868ECDECA94133A61A0FFA282E877')"
    elif mode == "'aes-256-ecb'":
        ciphertext = "unhex('1729E5354D6EC44D89900ABDB09DC297')"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    with When("key is too long"):
        if "ecb" in mode or "cbc" in mode:
            aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len+1]}'", mode=mode, exitcode=198, message="DB::Exception: Failed to decrypt")
        else:
            aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len+1]}'", mode=mode, cast="hex")

    if iv_len is not None:
        with When("iv is too short"):
            aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

        with When("iv is too long"):
            if "ecb" in mode or "cbc" in mode:
                aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len+1]}'", mode=mode, exitcode=198, message="DB::Exception: Failed to decrypt")
            else:
                aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len+1]}'", mode=mode, cast="hex")
    else:
        with When("iv is specified but not needed"):
            if "ecb" in mode or "cbc" in mode:
                aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", iv=f"'{iv}'", mode=mode, exitcode=198, message="DB::Exception: Failed to decrypt")
            else:
                aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", iv=f"'{iv}'", mode=mode)

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_InitializationVector("1.0")
)
def iv_parameter_types(self):
    """Check that `aes_decrypt_mysql` function accepts `iv` parameter as the fourth argument
    of either `String` or `FixedString` types.
    """
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("iv is specified using String type"):
        aes_decrypt_mysql(ciphertext="unhex('F024F9372FA0D8B974894D29FFB8A7F7')", key=key, mode=mode, iv=iv, message="hello there")

    with When("iv is specified using String with UTF8 characters"):
        aes_decrypt_mysql(ciphertext="unhex('7A4EC0FF3796F46BED281F4778ACE1DC')", key=key, mode=mode, iv="'Gãńdåłf_Thê'", message="hello there")

    with When("iv is specified using FixedString type"):
        aes_decrypt_mysql(ciphertext="unhex('F024F9372FA0D8B974894D29FFB8A7F7')", key=key, mode=mode, iv=f"toFixedString({iv}, 16)", message="hello there")

    with When("iv is specified using FixedString with UTF8 characters"):
        aes_decrypt_mysql(ciphertext="unhex('7A4EC0FF3796F46BED281F4778ACE1DC')", key=key, mode=mode, iv=f"toFixedString('Gãńdåłf_Thê', 16)", message="hello there")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Key("1.0")
)
def key_parameter_types(self):
    """Check that `aes_decrypt` function accepts `key` parameter as the second argument
    of either `String` or `FixedString` types.
    """
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("key is specified using String type"):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key=key, mode=mode, message="hello there")

    with When("key is specified using String with UTF8 characters"):
        aes_decrypt_mysql(ciphertext="unhex('180086AA42AD57B71C706EEC372D0C3D')", key="'Gãńdåłf_Thê'", mode=mode, message="hello there")

    with When("key is specified using FixedString type"):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key=f"toFixedString({key}, 16)", mode=mode, message="hello there")

    with When("key is specified using FixedString with UTF8 characters"):
        aes_decrypt_mysql(ciphertext="unhex('180086AA42AD57B71C706EEC372D0C3D')", key=f"toFixedString('Gãńdåłf_Thê', 16)", mode=mode, message="hello there")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode("1.0"),
)
def mode_parameter_types(self):
    """Check that `aes_decrypt_mysql` function accepts `mode` parameter as the third argument
    of either `String` or `FixedString` types.
    """
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("mode is specified using String type"):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key=key, mode=mode, message="hello there")

    with When("mode is specified using FixedString type"):
        aes_decrypt_mysql(ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')", key=key, mode=f"toFixedString({mode}, 12)", message="hello there")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_ReturnValue("1.0")
)
def return_value(self):
    """Check that `aes_decrypt_mysql` functions returns String data type.
    """
    ciphertext = "unhex('F024F9372FA0D8B974894D29FFB8A7F7')"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("I get type of the return value"):
        sql = "SELECT toTypeName(aes_decrypt_mysql(" + mode + "," + ciphertext + "," + key + "," + iv + "))"
        r = self.context.node.query(sql)

    with Then("type should be String"):
        assert r.output.strip() == "String", error()

    with When("I get the return value"):
        aes_decrypt_mysql(ciphertext=ciphertext, key=key, mode=mode, iv=iv, message="hello there")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Syntax("1.0"),
)
def syntax(self):
    """Check that `aes_decrypt_mysql` function supports syntax

    ```sql
    aes_decrypt_mysql(ciphertext, key, mode, [iv])
    ```
    """
    ciphertext = "70FE78410D6EE237C2DE4A"
    sql = f"SELECT aes_decrypt_mysql('aes-128-ofb', unhex('{ciphertext}'), '0123456789123456', '0123456789123456')"
    self.context.node.query(sql, step=When, message="hello there")

@TestScenario
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_CipherText("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_ValuesFormat("1.0"),
    RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Values("1.0")
)
def decryption(self):
    """Check that `aes_decrypt_mysql` functions accepts `mode` as the first parameter
    and `ciphertext` as the second parameter and we can convert the decrypted value into the original
    value with the original data type.
    """
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader("snapshot", os.path.join(current_dir(),
            "snapshots", "encrypt_mysql.py.encrypt_mysql.snapshot")).load_module()

    for mode, key_len, iv_len in mysql_modes:
        for datatype, plaintext in plaintexts:

            with Example(f"""mode={mode.strip("'")} datatype={datatype.strip("'")} key={key_len} iv={iv_len}""") as example:

                with Given("I have ciphertext"):
                    example_name = basename(example.name)
                    ciphertext = getattr(snapshot_module, varname(f"example_{example_name}"))

                cast = None
                endcast = None
                ciphertext = f"unhex({ciphertext})"
                compare = plaintext

                if datatype == "NULL" or datatype.endswith("Null"):
                    ciphertext = "NULL"
                    cast = "isNull"
                    compare = None

                aes_decrypt_mysql(ciphertext=ciphertext, key=f"'{key[:key_len]}'", mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                    cast=cast, endcast=endcast, compare=compare, message="1")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_Mismatched_Key("1.0")
)
def mismatched_key(self):
    """Check that `aes_decrypt_mysql` function returns garbage or an error when key parameter does not match.
    """
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader("snapshot", os.path.join(current_dir(),
            "snapshots", "encrypt_mysql.py.encrypt_mysql.snapshot")).load_module()

    for mode, key_len, iv_len in mysql_modes:
        with Example(f"""mode={mode.strip("'")} datatype=String key={key_len} iv={iv_len}""") as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(snapshot_module, varname(f"example_{example_name}"))

            with When("I decrypt using a mismatched key"):
                r = aes_decrypt_mysql(ciphertext=f"unhex({ciphertext})", key=f"'a{key[:key_len-1]}'", mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                    cast="hex", no_checks=True)

            with Then("exitcode shoud be 0 or 198"):
                assert r.exitcode in [0, 198], error()

            with And("output should be garbage or an error"):
                output = r.output.strip()
                assert "Exception: Failed to decrypt" in output or output != "31", error()

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_Mismatched_IV("1.0")
)
def mismatched_iv(self):
    """Check that `aes_decrypt_mysql` function returns garbage or an error when iv parameter does not match.
    """
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader("snapshot", os.path.join(current_dir(),
            "snapshots", "encrypt_mysql.py.encrypt_mysql.snapshot")).load_module()

    for mode, key_len, iv_len in mysql_modes:
        if not iv_len:
            continue
        with Example(f"""mode={mode.strip("'")} datatype=String key={key_len} iv={iv_len}""") as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(snapshot_module, varname(f"example_{example_name}"))

            with When("I decrypt using a mismatched key"):
                r = aes_decrypt_mysql(ciphertext=f"unhex({ciphertext})", key=f"'{key[:key_len]}'", mode=mode,
                    iv=f"'a{iv[:iv_len-1]}'",
                    cast="hex", no_checks=True)

            with Then("exitcode shoud be 0 or 198"):
                assert r.exitcode in [0, 198], error()

            with And("output should be garbage or an error"):
                output = r.output.strip()
                assert "Exception: Failed to decrypt" in output or output != "31", error()

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_Mismatched_Mode("1.0")
)
def mismatched_mode(self):
    """Check that `aes_decrypt_mysql` function returns garbage or an error when mode parameter does not match.
    """
    key = f"{'1' * 64}"
    iv = f"{'2' * 64}"
    plaintext = hex('Gãńdåłf_Thê_Gręât'.encode("utf-8"))

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader("snapshot", os.path.join(current_dir(),
            "snapshots", "encrypt_mysql.py.encrypt_mysql.snapshot")).load_module()

    for mode, key_len, iv_len in mysql_modes:
        if not iv_len:
            continue

        with Example(f"""mode={mode.strip("'")} datatype=utf8string key={key_len} iv={iv_len}""") as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(snapshot_module, varname(f"example_{example_name}"))

            for mismatched_mode, _, _ in mysql_modes:
                if mismatched_mode == mode:
                    continue

                with When(f"I decrypt using a mismatched mode {mismatched_mode}"):
                    r = aes_decrypt_mysql(ciphertext=f"unhex({ciphertext})", key=f"'{key[:key_len]}'", mode=mismatched_mode,
                        iv=f"'{iv[:iv_len]}'",
                        cast="hex", no_checks=True)

                    with Then("exitcode shoud be 0 or 36 or 198"):
                        assert r.exitcode in [0, 36, 198], error()

                    with And("output should be garbage or an error"):
                        output = r.output.strip()
                        assert "Exception: Failed to decrypt" in output or output != plaintext, error()

@TestFeature
@Name("decrypt_mysql")
@Requirements(
    RQ_SRS008_AES_MySQL_Decrypt_Function("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the behavior of the `aes_decrypt_mysql` function.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
