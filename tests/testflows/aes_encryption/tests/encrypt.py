# -*- coding: utf-8 -*-
from testflows.core import *
from testflows.core.name import basename
from testflows.asserts import values, error, snapshot

from aes_encryption.requirements.requirements import *
from aes_encryption.tests.common import *

@TestOutline
def encrypt(self, plaintext=None, key=None, mode=None, iv=None, aad=None, exitcode=0, message=None, step=When):
    """Execute `encrypt` function with the specified parameters.
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
    if aad is not None:
        params.append(aad)

    sql = "SELECT hex(encrypt(" + ", ".join(params) + "))"

    return current().context.node.query(sql, step=step, exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS008_AES_Functions_InvalidParameters("1.0")
)
def invalid_parameters(self):
    """Check that `encrypt` function returns an error when
    we call it with invalid parameters.
    """
    with Example("no parameters"):
        encrypt(exitcode=42, message="DB::Exception: Incorrect number of arguments for function encrypt provided 0, expected 3 to 5")

    with Example("missing key and mode"):
        encrypt(plaintext="'hello there'", exitcode=42, message="DB::Exception: Incorrect number of arguments for function encrypt provided 1")

    with Example("missing mode"):
        encrypt(plaintext="'hello there'", key="'123'", exitcode=42, message="DB::Exception: Incorrect number of arguments for function encrypt provided 2")

    with Example("bad key type - UInt8"):
        encrypt(plaintext="'hello there'", key="123", mode="'aes-128-ecb'", exitcode=43,
            message="DB::Exception: Received from localhost:9000. DB::Exception: Illegal type of argument #3")

    with Example("bad mode type - forgot quotes"):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="aes-128-ecb", exitcode=47,
            message="DB::Exception: Missing columns: 'ecb' 'aes' while processing query")

    with Example("bad mode type - UInt8"):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="128", exitcode=43,
            message="DB::Exception: Illegal type of argument #1 'mode'")

    with Example("bad iv type - UInt8"):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-cbc'", iv='128', exitcode=43,
            message="DB::Exception: Illegal type of argument")

    with Example("bad aad type - UInt8"):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-gcm'", iv="'012345678912'", aad="123", exitcode=43,
            message="DB::Exception: Illegal type of argument")

    with Example("iv not valid for mode", requirements=[RQ_SRS008_AES_Encrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-ecb'", iv="'012345678912'", exitcode=36,
            message="DB::Exception: aes-128-ecb does not support IV")

    with Example("iv not valid for mode - size 0", requirements=[RQ_SRS008_AES_Encrypt_Function_InitializationVector_NotValidForMode("1.0")]):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-ecb'", iv="''", exitcode=36,
            message="DB::Exception: aes-128-ecb does not support IV")

    with Example("aad not valid for mode", requirements=[RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_NotValidForMode("1.0")]):
        encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-cbc'", iv="'0123456789123456'", aad="'aad'", exitcode=36,
            message="DB::Exception: AAD can be only set for GCM-mode")

    with Example("invalid mode value", requirements=[RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_Invalid("1.0")]):
        with When("using unsupported cfb1 mode"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-cfb1'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-cfb1")

        with When("using unsupported cfb8 mode"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-cfb8'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-cfb8")

        with When("typo in the block algorithm"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128-eeb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-eeb")

        with When("typo in the key size"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-127-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-127-ecb")

        with When("typo in the aes prefix"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aee-128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aee-128-ecb")

        with When("missing last dash"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes-128ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes-128ecb")

        with When("missing first dash"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'aes128-ecb'", exitcode=36,
                message="DB::Exception: Invalid mode: aes128-ecb")

        with When("all capitals"):
            encrypt(plaintext="'hello there'", key="'0123456789123456'", mode="'AES-128-ECB'", exitcode=36,
                message="DB::Exception: Invalid mode: AES-128-ECB")

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_Functions_InvalidParameters("1.0")
)
@Examples("data_type, value", [
    ("UInt8", "toUInt8('1')"),
    ("UInt16", "toUInt16('1')"),
    ("UInt32", "toUInt32('1')"),
    ("UInt64", "toUInt64('1')"),
    ("Int8", "toInt8('1')"),
    ("Int16", "toInt16('1')"),
    ("Int32", "toInt32('1')"),
    ("Int64", "toInt64('1')"),
    ("Float32", "toFloat32('1.0')"),
    ("Float64", "toFloat64('1.0')"),
    ("Decimal32", "toDecimal32(2, 4)"),
    ("Decimal64", "toDecimal64(2, 4)"),
    ("Decimal128", "toDecimal128(2, 4)"),
    ("UUID", "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')"),
    ("Date", "toDate('2020-01-01')"),
    ("DateTime", "toDateTime('2020-01-01 20:01:02')"),
    ("DateTime64", "toDateTime64('2020-01-01 20:01:02.123', 3)"),
    ("Array", "[1,2]"),
    ("Tuple", "(1,'a')"),
    ("IPv4", "toIPv4('171.225.130.45')"),
    ("IPv6", "toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001')"),
    ("Enum8", r"CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)')"),
    ("Enum16", r"CAST('a', 'Enum16(\'a\' = 1, \'b\' = 2)')")
])
def invalid_plaintext_data_type(self, data_type, value):
    """Check that encrypt function returns an error if the
    plaintext parameter has invalid data type.
    """
    with When("I try to encrypt plaintext with invalid data type", description=f"{data_type} with value {value}"):
        encrypt(plaintext=value, key="'0123456789123456'", mode="'aes-128-cbc'", iv="'0123456789123456'",
            exitcode=43, message="DB::Exception: Illegal type of argument")

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Key_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Encrypt_Function_InitializationVector_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_NotValidForMode("1.0"),
    RQ_SRS008_AES_Encrypt_Function_NonGCMMode_KeyAndInitializationVector_Length("1.0")
)
@Examples("mode key_len iv_len aad", [
    # ECB
    ("'aes-128-ecb'", 16, None, None),
    ("'aes-192-ecb'", 24, None, None),
    ("'aes-256-ecb'", 32, None, None),
    # CBC
    ("'aes-128-cbc'", 16, 16, None),
    ("'aes-192-cbc'", 24, 16, None),
    ("'aes-256-cbc'", 32, 16, None),
    # CFB128
    ("'aes-128-cfb128'", 16, 16, None),
    ("'aes-192-cfb128'", 24, 16, None),
    ("'aes-256-cfb128'", 32, 16, None),
    # OFB
    ("'aes-128-ofb'", 16, 16, None),
    ("'aes-192-ofb'", 24, 16, None),
    ("'aes-256-ofb'", 32, 16, None),
    # CTR
    ("'aes-128-ctr'", 16, 16, None),
    ("'aes-192-ctr'", 24, 16, None),
    ("'aes-256-ctr'", 32, 16, None),
], "%-16s %-10s %-10s %-10s")
def invalid_key_or_iv_length_for_mode_non_gcm(self, mode, key_len, iv_len, aad):
    """Check that an error is returned when key or iv length does not match
    the expected value for the mode.
    """
    plaintext = "'hello there'"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        encrypt(plaintext=plaintext, key=f"'{key[:key_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    with When("key is too long"):
        encrypt(plaintext=plaintext, key=f"'{key[:key_len+1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    if iv_len is not None:
        with When("iv is too short"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len-1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

        with When("iv is too long"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len+1]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

        if aad is None:
            with When("aad is specified but not needed"):
                encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len+1] if iv_len is not None else ''}'", aad="'AAD'", mode=mode, exitcode=36, message="DB::Exception: AAD can be only set for GCM-mode")

    else:
        with When("iv is specified but not needed"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv}'", mode=mode, exitcode=36, message="DB::Exception: {} does not support IV".format(mode.strip("'")))

@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Key_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Encrypt_Function_InitializationVector_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_NotValidForMode("1.0"),
    RQ_SRS008_AES_Encrypt_Function_GCMMode_KeyAndInitializationVector_Length("1.0")
)
@Examples("mode key_len iv_len aad", [
    # GCM
    ("'aes-128-gcm'", 16, 8, "'hello there aad'"),
    ("'aes-192-gcm'", 24, 8, "''"),
    ("'aes-256-gcm'", 32, 8, "'a'"),
], "%-16s %-10s %-10s %-10s")
def invalid_key_or_iv_length_for_gcm(self, mode, key_len, iv_len, aad):
    """Check that an error is returned when key or iv length does not match
    the expected value for the GCM mode.
    """
    plaintext = "'hello there'"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        encrypt(plaintext=plaintext, key=f"'{key[:key_len-1]}'", iv=f"'{iv[:iv_len]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    with When("key is too long"):
        encrypt(plaintext=plaintext, key=f"'{key[:key_len+1]}'", iv=f"'{iv[:iv_len]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid key size")

    if iv_len is not None:
        with When(f"iv is too short"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len-1]}'", mode=mode, exitcode=0)
    else:
        with When("iv is not specified"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", mode=mode, exitcode=36, message="DB::Exception: Invalid IV size")

    if aad is not None:
        with When(f"aad is {aad}"):
            encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", iv=f"'{iv[:iv_len]}'", aad=f"{aad}", mode=mode)

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_AdditionalAuthenticatedData("1.0"),
    RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_Length("1.0")
)
def aad_parameter_types_and_length(self):
    """Check that `encrypt` function accepts `aad` parameter as the fifth argument
    of either `String` or `FixedString` types and that the length is not limited.
    """
    plaintext = "'hello there'"
    iv = "'012345678912'"
    mode = "'aes-128-gcm'"
    key = "'0123456789123456'"

    with When("aad is specified using String type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="'aad'", message="19A1183335B374C626B24208AAEC97F148732CE05621AC87B21526")

    with When("aad is specified using String with UTF8 characters"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="'Gãńdåłf_Thê_Gręât'", message="19A1183335B374C626B242C68D9618A8C2664D7B6A3FE978104B39")

    with When("aad is specified using FixedString type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="toFixedString('aad', 3)", message="19A1183335B374C626B24208AAEC97F148732CE05621AC87B21526")

    with When("aad is specified using FixedString with UTF8 characters"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="toFixedString('Gãńdåłf_Thê_Gręât', 24)", message="19A1183335B374C626B242C68D9618A8C2664D7B6A3FE978104B39")

    with When("aad is 0 bytes"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="''", message="19A1183335B374C626B242DF92BB3F57F5D82BEDF41FD5D49F8BC9")

    with When("aad is 1 byte"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad="'1'", message="19A1183335B374C626B242D1BCFC63B09CFE9EAD20285044A01035")

    with When("aad is 256 bytes"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, aad=f"'{'1' * 256}'", message="19A1183335B374C626B242355AD3DD2C5D7E36AEECBB847BF9E8A7")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_InitializationVector("1.0")
)
def iv_parameter_types(self):
    """Check that `encrypt` function accepts `iv` parameter as the fourth argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("iv is specified using String type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, message="F024F9372FA0D8B974894D29FFB8A7F7")

    with When("iv is specified using String with UTF8 characters"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv="'Gãńdåłf_Thê'", message="7A4EC0FF3796F46BED281F4778ACE1DC")

    with When("iv is specified using FixedString type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=f"toFixedString({iv}, 16)", message="F024F9372FA0D8B974894D29FFB8A7F7")

    with When("iv is specified using FixedString with UTF8 characters"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv="toFixedString('Gãńdåłf_Thê', 16)", message="7A4EC0FF3796F46BED281F4778ACE1DC")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_Key("1.0")
)
def key_parameter_types(self):
    """Check that `encrypt` function accepts `key` parameter as the second argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("key is specified using String type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("key is specified using String with UTF8 characters"):
        encrypt(plaintext=plaintext, key="'Gãńdåłf_Thê'", mode=mode, message="180086AA42AD57B71C706EEC372D0C3D")

    with When("key is specified using FixedString type"):
        encrypt(plaintext=plaintext, key=f"toFixedString({key}, 16)", mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("key is specified using FixedString with UTF8 characters"):
        encrypt(plaintext=plaintext, key="toFixedString('Gãńdåłf_Thê', 16)", mode=mode, message="180086AA42AD57B71C706EEC372D0C3D")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_Mode("1.0"),
)
def mode_parameter_types(self):
    """Check that `encrypt` function accepts `mode` parameter as the third argument
    of either `String` or `FixedString` types.
    """
    plaintext = "'hello there'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("mode is specified using String type"):
        encrypt(plaintext=plaintext, key=key, mode=mode, message="49C9ADB81BA9B58C485E7ADB90E70576")

    with When("mode is specified using FixedString type"):
        encrypt(plaintext=plaintext, key=key, mode=f"toFixedString({mode}, 12)", message="49C9ADB81BA9B58C485E7ADB90E70576")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_PlainText("2.0"),
    RQ_SRS008_AES_Encrypt_Function_Parameters_Mode("1.0"),
    RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_ValuesFormat("1.0"),
    RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Values("1.0")
)
def encryption(self):
    """Check that `encrypt` functions accepts `plaintext` as the second parameter
    with any data type and `mode` as the first parameter.
    """
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    for mode, key_len, iv_len, aad_len in modes:
        for datatype, plaintext in plaintexts:
            with Example(f"""mode={mode.strip("'")} datatype={datatype.strip("'")} iv={iv_len} aad={aad_len}""") as example:

                r = encrypt(plaintext=plaintext, key=f"'{key[:key_len]}'", mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"), aad=(None if not aad_len else f"'{aad}'"))

                with Then("I check output against snapshot"):
                    with values() as that:
                        example_name = basename(example.name)
                        assert that(snapshot(r.output.strip(), "encrypt", name=f"example_{example_name.replace(' ', '_')}")), error()

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Parameters_ReturnValue("1.0")
)
def return_value(self):
    """Check that `encrypt` functions returns String data type.
    """
    plaintext = "'hello there'"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("I get type of the return value"):
        sql = "SELECT toTypeName(encrypt(" + mode + "," + plaintext + "," + key + "," + iv + "))"
        r = self.context.node.query(sql)

    with Then("type should be String"):
        assert r.output.strip() == "String", error()

    with When("I get return ciphertext as hex"):
        encrypt(plaintext=plaintext, key=key, mode=mode, iv=iv, message="F024F9372FA0D8B974894D29FFB8A7F7")

@TestScenario
@Requirements(
    RQ_SRS008_AES_Encrypt_Function_Syntax("1.0"),
)
def syntax(self):
    """Check that `encrypt` function supports syntax

    ```sql
    encrypt(plaintext, key, mode, [iv, aad])
    ```
    """
    sql = "SELECT hex(encrypt('aes-128-gcm', 'hello there', '0123456789123456', '012345678912', 'AAD'))"
    self.context.node.query(sql, step=When, message="19A1183335B374C626B242A6F6E8712E2B64DCDC6A468B2F654614")

@TestFeature
@Name("encrypt")
@Requirements(
    RQ_SRS008_AES_Encrypt_Function("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the behavior of the `encrypt` function.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
