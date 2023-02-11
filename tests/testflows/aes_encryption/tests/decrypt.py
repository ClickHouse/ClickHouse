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
def decrypt(
    self,
    ciphertext=None,
    key=None,
    mode=None,
    iv=None,
    aad=None,
    exitcode=0,
    message=None,
    step=When,
    cast=None,
    endcast=None,
    compare=None,
    no_checks=False,
):
    """Execute `decrypt` function with the specified parameters."""
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

    sql = f"decrypt(" + ", ".join(params) + ")"
    if cast:
        sql = f"{cast}({sql}){endcast or ''}"
    if compare:
        sql = f"{compare} = {sql}"
    sql = f"SELECT {sql}"

    return current().context.node.query(
        sql, step=step, exitcode=exitcode, message=message, no_checks=no_checks
    )


@TestScenario
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Parameters_CipherText("1.0"),
)
def invalid_ciphertext(self):
    """Check that `decrypt` function does not crash when invalid
    `ciphertext` is passed as the first parameter.
    """
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"
    invalid_ciphertexts = plaintexts

    for mode, key_len, iv_len, aad_len in modes:
        with Example(f"""mode={mode.strip("'")} iv={iv_len} aad={aad_len}"""):
            d_iv = None if not iv_len else f"'{iv[:iv_len]}'"
            d_aad = None if not aad_len else f"'{aad}'"

            for datatype, ciphertext in invalid_ciphertexts:
                if datatype == "NULL" or datatype.endswith("Null"):
                    continue
                with When(f"invalid ciphertext={ciphertext}"):
                    if "cfb" in mode or "ofb" in mode or "ctr" in mode:
                        decrypt(
                            ciphertext=ciphertext,
                            key=f"'{key[:key_len]}'",
                            mode=mode,
                            iv=d_iv,
                            aad=d_aad,
                            cast="hex",
                        )
                    else:
                        with When("I execute decrypt function"):
                            r = decrypt(
                                ciphertext=ciphertext,
                                key=f"'{key[:key_len]}'",
                                mode=mode,
                                iv=d_iv,
                                aad=d_aad,
                                no_checks=True,
                                step=By,
                            )
                        with Then("exitcode is not zero"):
                            assert r.exitcode in [198, 36]
                        with And("exception is present in the output"):
                            assert "DB::Exception:" in r.output


@TestScenario
@Requirements(RQ_SRS008_AES_Functions_InvalidParameters("1.0"))
def invalid_parameters(self):
    """Check that `decrypt` function returns an error when
    we call it with invalid parameters.
    """
    ciphertext = "unhex('AA1826B5F66A903C888D5DCDA9FB63D1D9CCA10EC55F59D6C00D37')"

    with Example("no parameters"):
        decrypt(
            exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function decrypt provided 0, expected 3 to 5",
        )

    with Example("missing key and mode"):
        decrypt(
            ciphertext=ciphertext,
            exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function decrypt provided 1",
        )

    with Example("missing mode"):
        decrypt(
            ciphertext=ciphertext,
            key="'123'",
            exitcode=42,
            message="DB::Exception: Incorrect number of arguments for function decrypt provided 2",
        )

    with Example("bad key type - UInt8"):
        decrypt(
            ciphertext=ciphertext,
            key="123",
            mode="'aes-128-ecb'",
            exitcode=43,
            message="DB::Exception: Received from localhost:9000. DB::Exception: Illegal type of argument #3",
        )

    with Example("bad mode type - forgot quotes"):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="aes-128-ecb",
            exitcode=47,
            message="DB::Exception: Missing columns: 'ecb' 'aes' while processing query",
        )

    with Example("bad mode type - UInt8"):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="128",
            exitcode=43,
            message="DB::Exception: Illegal type of argument #1 'mode'",
        )

    with Example("bad iv type - UInt8"):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="'aes-128-cbc'",
            iv="128",
            exitcode=43,
            message="DB::Exception: Illegal type of argument",
        )

    with Example("bad aad type - UInt8"):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="'aes-128-gcm'",
            iv="'012345678912'",
            aad="123",
            exitcode=43,
            message="DB::Exception: Illegal type of argument",
        )

    with Example(
        "iv not valid for mode",
        requirements=[
            RQ_SRS008_AES_Decrypt_Function_InitializationVector_NotValidForMode("1.0")
        ],
    ):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="'aes-128-ecb'",
            iv="'012345678912'",
            exitcode=36,
            message="DB::Exception: aes-128-ecb does not support IV",
        )

    with Example(
        "iv not valid for mode - size 0",
        requirements=[
            RQ_SRS008_AES_Decrypt_Function_InitializationVector_NotValidForMode("1.0")
        ],
    ):
        decrypt(
            ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')",
            key="'0123456789123456'",
            mode="'aes-128-ecb'",
            iv="''",
            exitcode=36,
            message="DB::Exception: aes-128-ecb does not support IV",
        )

    with Example(
        "aad not valid for mode",
        requirements=[
            RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_NotValidForMode(
                "1.0"
            )
        ],
    ):
        decrypt(
            ciphertext=ciphertext,
            key="'0123456789123456'",
            mode="'aes-128-cbc'",
            iv="'0123456789123456'",
            aad="'aad'",
            exitcode=36,
            message="DB::Exception: AAD can be only set for GCM-mode",
        )

    with Example(
        "invalid mode value",
        requirements=[
            RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_Invalid("1.0")
        ],
    ):
        with When("using unsupported cfb1 mode"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes-128-cfb1'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-cfb1",
            )

        with When("using unsupported cfb8 mode"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes-128-cfb8'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-cfb8",
            )

        with When("typo in the block algorithm"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes-128-eeb'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes-128-eeb",
            )

        with When("typo in the key size"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes-127-ecb'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes-127-ecb",
            )

        with When("typo in the aes prefix"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aee-128-ecb'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aee-128-ecb",
            )

        with When("missing last dash"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes-128ecb'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes-128ecb",
            )

        with When("missing first dash"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'aes128-ecb'",
                exitcode=36,
                message="DB::Exception: Invalid mode: aes128-ecb",
            )

        with When("all capitals"):
            decrypt(
                ciphertext=ciphertext,
                key="'0123456789123456'",
                mode="'AES-128-ECB'",
                exitcode=36,
                message="DB::Exception: Invalid mode: AES-128-ECB",
            )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Key_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Decrypt_Function_InitializationVector_Length_InvalidLengthError(
        "1.0"
    ),
    RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_NotValidForMode("1.0"),
    RQ_SRS008_AES_Decrypt_Function_NonGCMMode_KeyAndInitializationVector_Length("1.0"),
)
@Examples(
    "mode key_len iv_len aad",
    [
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
    ],
    "%-16s %-10s %-10s %-10s",
)
def invalid_key_or_iv_length_for_mode_non_gcm(self, mode, key_len, iv_len, aad):
    """Check that an error is returned when key or iv length does not match
    the expected value for the mode.
    """
    ciphertext = "unhex('AA1826B5F66A903C888D5DCDA9FB63D1D9CCA10EC55F59D6C00D37')"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        decrypt(
            ciphertext=ciphertext,
            key=f"'{key[:key_len-1]}'",
            mode=mode,
            exitcode=36,
            message="DB::Exception: Invalid key size",
        )

    with When("key is too long"):
        decrypt(
            ciphertext=ciphertext,
            key=f"'{key[:key_len+1]}'",
            mode=mode,
            exitcode=36,
            message="DB::Exception: Invalid key size",
        )

    if iv_len is not None:
        with When("iv is too short"):
            decrypt(
                ciphertext=ciphertext,
                key=f"'{key[:key_len]}'",
                iv=f"'{iv[:iv_len-1]}'",
                mode=mode,
                exitcode=36,
                message="DB::Exception: Invalid IV size",
            )

        with When("iv is too long"):
            decrypt(
                ciphertext=ciphertext,
                key=f"'{key[:key_len]}'",
                iv=f"'{iv[:iv_len+1]}'",
                mode=mode,
                exitcode=36,
                message="DB::Exception: Invalid IV size",
            )

        if aad is None:
            with When("aad is specified but not needed"):
                decrypt(
                    ciphertext=ciphertext,
                    key=f"'{key[:key_len]}'",
                    iv=f"'{iv[:iv_len+1] if iv_len is not None else ''}'",
                    aad="'AAD'",
                    mode=mode,
                    exitcode=36,
                    message="DB::Exception: AAD can be only set for GCM-mode",
                )

    else:
        with When("iv is specified but not needed"):
            decrypt(
                ciphertext=ciphertext,
                key=f"'{key[:key_len]}'",
                iv=f"'{iv}'",
                mode=mode,
                exitcode=36,
                message="DB::Exception: {} does not support IV".format(mode.strip("'")),
            )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Key_Length_InvalidLengthError("1.0"),
    RQ_SRS008_AES_Decrypt_Function_InitializationVector_Length_InvalidLengthError(
        "1.0"
    ),
    RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_NotValidForMode("1.0"),
    RQ_SRS008_AES_Decrypt_Function_GCMMode_KeyAndInitializationVector_Length("1.0"),
)
@Examples(
    "mode key_len iv_len aad",
    [
        # GCM
        ("'aes-128-gcm'", 16, 8, "'hello there aad'"),
        ("'aes-128-gcm'", 16, None, "'hello there aad'"),
        ("'aes-192-gcm'", 24, 8, "''"),
        ("'aes-192-gcm'", 24, None, "''"),
        ("'aes-256-gcm'", 32, 8, "'a'"),
        ("'aes-256-gcm'", 32, None, "'a'"),
    ],
    "%-16s %-10s %-10s %-10s",
)
def invalid_key_or_iv_length_for_gcm(self, mode, key_len, iv_len, aad):
    """Check that an error is returned when key or iv length does not match
    the expected value for the GCM mode.
    """
    ciphertext = "'hello there'"
    plaintext = "hello there"
    key = "0123456789" * 4
    iv = "0123456789" * 4

    with When("key is too short"):
        ciphertext = "unhex('AA1826B5F66A903C888D5DCDA9FB63D1D9CCA10EC55F59D6C00D37')"
        decrypt(
            ciphertext=ciphertext,
            key=f"'{key[:key_len-1]}'",
            iv=f"'{iv[:iv_len]}'",
            mode=mode,
            exitcode=36,
            message="DB::Exception: Invalid key size",
        )

    with When("key is too long"):
        ciphertext = "unhex('24AEBFEA049D6F4CF85AAB8CADEDF39CCCAA1C3C2AFF99E194789D')"
        decrypt(
            ciphertext=ciphertext,
            key=f"'{key[:key_len+1]}'",
            iv=f"'{iv[:iv_len]}'",
            mode=mode,
            exitcode=36,
            message="DB::Exception: Invalid key size",
        )

    if iv_len is not None:
        with When(f"iv is too short"):
            ciphertext = (
                "unhex('24AEBFEA049D6F4CF85AAB8CADEDF39CCCAA1C3C2AFF99E194789D')"
            )
            decrypt(
                ciphertext=ciphertext,
                key=f"'{key[:key_len]}'",
                iv=f"'{iv[:iv_len-1]}'",
                mode=mode,
                exitcode=198,
                message="DB::Exception:",
            )
    else:
        with When("iv is not specified"):
            ciphertext = (
                "unhex('1CD4EC93A4B0C687926E8F8C2AA3B4CE1943D006DAE3A774CB1AE5')"
            )
            decrypt(
                ciphertext=ciphertext,
                key=f"'{key[:key_len]}'",
                mode=mode,
                exitcode=36,
                message="DB::Exception: Invalid IV size 0 != expected size 12",
            )


@TestScenario
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Parameters_AdditionalAuthenticatedData("1.0"),
    RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_Length("1.0"),
)
def aad_parameter_types_and_length(self):
    """Check that `decrypt` function accepts `aad` parameter as the fifth argument
    of either `String` or `FixedString` types and that the length is not limited.
    """
    plaintext = "hello there"
    iv = "'012345678912'"
    mode = "'aes-128-gcm'"
    key = "'0123456789123456'"

    with When("aad is specified using String type"):
        ciphertext = "unhex('19A1183335B374C626B24208AAEC97F148732CE05621AC87B21526')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="'aad'",
            message=plaintext,
        )

    with When("aad is specified using String with UTF8 characters"):
        ciphertext = "unhex('19A1183335B374C626B242C68D9618A8C2664D7B6A3FE978104B39')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="'Gãńdåłf_Thê_Gręât'",
            message=plaintext,
        )

    with When("aad is specified using FixedString type"):
        ciphertext = "unhex('19A1183335B374C626B24208AAEC97F148732CE05621AC87B21526')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="toFixedString('aad', 3)",
            message=plaintext,
        )

    with When("aad is specified using FixedString with UTF8 characters"):
        ciphertext = "unhex('19A1183335B374C626B242C68D9618A8C2664D7B6A3FE978104B39')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="toFixedString('Gãńdåłf_Thê_Gręât', 24)",
            message=plaintext,
        )

    with When("aad is 0 bytes"):
        ciphertext = "unhex('19A1183335B374C626B242DF92BB3F57F5D82BEDF41FD5D49F8BC9')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="''",
            message=plaintext,
        )

    with When("aad is 1 byte"):
        ciphertext = "unhex('19A1183335B374C626B242D1BCFC63B09CFE9EAD20285044A01035')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad="'1'",
            message=plaintext,
        )

    with When("aad is 256 bytes"):
        ciphertext = "unhex('19A1183335B374C626B242355AD3DD2C5D7E36AEECBB847BF9E8A7')"
        decrypt(
            ciphertext=ciphertext,
            key=key,
            mode=mode,
            iv=iv,
            aad=f"'{'1' * 256}'",
            message=plaintext,
        )


@TestScenario
@Requirements(RQ_SRS008_AES_Decrypt_Function_Parameters_InitializationVector("1.0"))
def iv_parameter_types(self):
    """Check that `decrypt` function accepts `iv` parameter as the fourth argument
    of either `String` or `FixedString` types.
    """
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("iv is specified using String type"):
        decrypt(
            ciphertext="unhex('F024F9372FA0D8B974894D29FFB8A7F7')",
            key=key,
            mode=mode,
            iv=iv,
            message="hello there",
        )

    with When("iv is specified using String with UTF8 characters"):
        decrypt(
            ciphertext="unhex('7A4EC0FF3796F46BED281F4778ACE1DC')",
            key=key,
            mode=mode,
            iv="'Gãńdåłf_Thê'",
            message="hello there",
        )

    with When("iv is specified using FixedString type"):
        decrypt(
            ciphertext="unhex('F024F9372FA0D8B974894D29FFB8A7F7')",
            key=key,
            mode=mode,
            iv=f"toFixedString({iv}, 16)",
            message="hello there",
        )

    with When("iv is specified using FixedString with UTF8 characters"):
        decrypt(
            ciphertext="unhex('7A4EC0FF3796F46BED281F4778ACE1DC')",
            key=key,
            mode=mode,
            iv=f"toFixedString('Gãńdåłf_Thê', 16)",
            message="hello there",
        )


@TestScenario
@Requirements(RQ_SRS008_AES_Decrypt_Function_Parameters_Key("1.0"))
def key_parameter_types(self):
    """Check that `decrypt` function accepts `key` parameter as the second argument
    of either `String` or `FixedString` types.
    """
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("key is specified using String type"):
        decrypt(
            ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')",
            key=key,
            mode=mode,
            message="hello there",
        )

    with When("key is specified using String with UTF8 characters"):
        decrypt(
            ciphertext="unhex('180086AA42AD57B71C706EEC372D0C3D')",
            key="'Gãńdåłf_Thê'",
            mode=mode,
            message="hello there",
        )

    with When("key is specified using FixedString type"):
        decrypt(
            ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')",
            key=f"toFixedString({key}, 16)",
            mode=mode,
            message="hello there",
        )

    with When("key is specified using FixedString with UTF8 characters"):
        decrypt(
            ciphertext="unhex('180086AA42AD57B71C706EEC372D0C3D')",
            key=f"toFixedString('Gãńdåłf_Thê', 16)",
            mode=mode,
            message="hello there",
        )


@TestScenario
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Parameters_Mode("1.0"),
)
def mode_parameter_types(self):
    """Check that `decrypt` function accepts `mode` parameter as the third argument
    of either `String` or `FixedString` types.
    """
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("mode is specified using String type"):
        decrypt(
            ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')",
            key=key,
            mode=mode,
            message="hello there",
        )

    with When("mode is specified using FixedString type"):
        decrypt(
            ciphertext="unhex('49C9ADB81BA9B58C485E7ADB90E70576')",
            key=key,
            mode=f"toFixedString({mode}, 12)",
            message="hello there",
        )


@TestScenario
@Requirements(RQ_SRS008_AES_Decrypt_Function_Parameters_ReturnValue("1.0"))
def return_value(self):
    """Check that `decrypt` functions returns String data type."""
    ciphertext = "unhex('F024F9372FA0D8B974894D29FFB8A7F7')"
    iv = "'0123456789123456'"
    mode = "'aes-128-cbc'"
    key = "'0123456789123456'"

    with When("I get type of the return value"):
        sql = (
            "SELECT toTypeName(decrypt("
            + mode
            + ","
            + ciphertext
            + ","
            + key
            + ","
            + iv
            + "))"
        )
        r = self.context.node.query(sql)

    with Then("type should be String"):
        assert r.output.strip() == "String", error()

    with When("I get the return value"):
        decrypt(ciphertext=ciphertext, key=key, mode=mode, iv=iv, message="hello there")


@TestScenario
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Syntax("1.0"),
)
def syntax(self):
    """Check that `decrypt` function supports syntax

    ```sql
    decrypt(ciphertext, key, mode, [iv, aad])
    ```
    """
    ciphertext = "19A1183335B374C626B242A6F6E8712E2B64DCDC6A468B2F654614"
    sql = f"SELECT decrypt('aes-128-gcm', unhex('{ciphertext}'), '0123456789123456', '012345678912', 'AAD')"
    self.context.node.query(sql, step=When, message="hello there")


@TestScenario
@Requirements(
    RQ_SRS008_AES_Decrypt_Function_Parameters_CipherText("1.0"),
    RQ_SRS008_AES_Decrypt_Function_Parameters_Mode("1.0"),
    RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_ValuesFormat("1.0"),
    RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Values("1.0"),
)
def decryption(self):
    """Check that `decrypt` functions accepts `ciphertext` as the second parameter
    and `mode` as the first parameter and we can convert the decrypted value into the original
    value with the original data type.
    """
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "encrypt.py.encrypt.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        for datatype, plaintext in plaintexts:

            with Example(
                f"""mode={mode.strip("'")} datatype={datatype.strip("'")} iv={iv_len} aad={aad_len}"""
            ) as example:

                with Given("I have ciphertext"):
                    example_name = basename(example.name)
                    ciphertext = getattr(
                        snapshot_module, varname(f"example_{example_name}")
                    )

                cast = None
                endcast = None
                ciphertext = f"unhex({ciphertext})"
                compare = plaintext

                if datatype == "NULL" or datatype.endswith("Null"):
                    ciphertext = "NULL"
                    cast = "isNull"
                    compare = None

                decrypt(
                    ciphertext=ciphertext,
                    key=f"'{key[:key_len]}'",
                    mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                    aad=(None if not aad_len else f"'{aad}'"),
                    cast=cast,
                    endcast=endcast,
                    compare=compare,
                    message="1",
                )


@TestScenario
@Requirements(RQ_SRS008_AES_Functions_Mismatched_Key("1.0"))
def mismatched_key(self):
    """Check that `decrypt` function returns garbage or an error when key parameter does not match."""
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"
    datatype = "String"
    plaintext = "'1'"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "encrypt.py.encrypt.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} datatype={datatype.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(
                    snapshot_module, varname(f"example_{example_name}")
                )

            with When("I decrypt using a mismatched key"):
                r = decrypt(
                    ciphertext=f"unhex({ciphertext})",
                    key=f"'a{key[:key_len-1]}'",
                    mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                    aad=(None if not aad_len else f"'{aad}'"),
                    no_checks=True,
                    cast="hex",
                )

            with Then("exitcode shoud be 0 or 198"):
                assert r.exitcode in [0, 198], error()

            with And("output should be garbage or an error"):
                output = r.output.strip()
                assert (
                    "Exception: Failed to decrypt" in output or output != "31"
                ), error()


@TestScenario
@Requirements(RQ_SRS008_AES_Functions_Mismatched_IV("1.0"))
def mismatched_iv(self):
    """Check that `decrypt` function returns garbage or an error when iv parameter does not match."""
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"
    datatype = "String"
    plaintext = "'1'"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "encrypt.py.encrypt.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        if not iv_len:
            continue
        with Example(
            f"""mode={mode.strip("'")} datatype={datatype.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(
                    snapshot_module, varname(f"example_{example_name}")
                )

            with When("I decrypt using a mismatched iv"):
                r = decrypt(
                    ciphertext=f"unhex({ciphertext})",
                    key=f"'{key[:key_len]}'",
                    mode=mode,
                    iv=f"'a{iv[:iv_len-1]}'",
                    aad=(None if not aad_len else f"'{aad}'"),
                    no_checks=True,
                    cast="hex",
                )

            with Then("exitcode shoud be 0 or 198"):
                assert r.exitcode in [0, 198], error()

            with And("output should be garbage or an error"):
                output = r.output.strip()
                assert (
                    "Exception: Failed to decrypt" in output or output != "31"
                ), error()


@TestScenario
@Requirements(RQ_SRS008_AES_Functions_Mismatched_AAD("1.0"))
def mismatched_aad(self):
    """Check that `decrypt` function returns garbage or an error when aad parameter does not match."""
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"
    datatype = "String"
    plaintext = "'1'"

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "encrypt.py.encrypt.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        if not aad_len:
            continue
        with Example(
            f"""mode={mode.strip("'")} datatype={datatype.strip("'")} iv={iv_len} aad={aad_len}"""
        ) as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(
                    snapshot_module, varname(f"example_{example_name}")
                )

            with When("I decrypt using a mismatched aad"):
                r = decrypt(
                    ciphertext=f"unhex({ciphertext})",
                    key=f"'{key[:key_len]}'",
                    mode=mode,
                    iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                    aad=(None if not aad_len else f"'a{aad}'"),
                    no_checks=True,
                    cast="hex",
                )

            with Then("exitcode shoud be 0 or 198"):
                assert r.exitcode in [0, 198], error()

            with And("output should be garbage or an error"):
                output = r.output.strip()
                assert (
                    "Exception: Failed to decrypt" in output or output != "31"
                ), error()


@TestScenario
@Requirements(RQ_SRS008_AES_Functions_Mismatched_Mode("1.0"))
def mismatched_mode(self):
    """Check that `decrypt` function returns garbage or an error when mode parameter does not match."""
    key = f"{'1' * 36}"
    iv = f"{'2' * 16}"
    aad = "some random aad"
    plaintext = hex("Gãńdåłf_Thê_Gręât".encode("utf-8"))

    with Given("I load encrypt snapshots"):
        snapshot_module = SourceFileLoader(
            "snapshot",
            os.path.join(current_dir(), "snapshots", "encrypt.py.encrypt.snapshot"),
        ).load_module()

    for mode, key_len, iv_len, aad_len in modes:
        with Example(
            f"""mode={mode.strip("'")} datatype=utf8string iv={iv_len} aad={aad_len}"""
        ) as example:
            with Given("I have ciphertext"):
                example_name = basename(example.name)
                ciphertext = getattr(
                    snapshot_module, varname(f"example_{example_name}")
                )

            for mismatched_mode, _, _, _ in modes:
                if mismatched_mode == mode:
                    continue

                with When(f"I decrypt using mismatched mode {mismatched_mode}"):
                    r = decrypt(
                        ciphertext=f"unhex({ciphertext})",
                        key=f"'{key[:key_len]}'",
                        mode=mismatched_mode,
                        iv=(None if not iv_len else f"'{iv[:iv_len]}'"),
                        aad=(None if not aad_len else f"'{aad}'"),
                        no_checks=True,
                        cast="hex",
                    )

                    with Then("exitcode shoud be 0 or 36 or 198"):
                        assert r.exitcode in [0, 36, 198], error()

                    with And("output should be garbage or an error"):
                        output = r.output.strip()
                        condition = (
                            "Exception: Failed to decrypt" in output
                            or "Exception: Invalid key size" in output
                            or output != plaintext
                        )
                        assert condition, error()


@TestFeature
@Name("decrypt")
@Requirements(RQ_SRS008_AES_Decrypt_Function("1.0"))
def feature(self, node="clickhouse1"):
    """Check the behavior of the `decrypt` function."""
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
