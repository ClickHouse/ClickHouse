# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.200731.1222107.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Requirement

RQ_SRS008_AES_Functions = Requirement(
        name='RQ.SRS008.AES.Functions',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support [AES] encryption functions to encrypt and decrypt data.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Compatability_MySQL = Requirement(
        name='RQ.SRS008.AES.Functions.Compatability.MySQL',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support [AES] encryption functions compatible with [MySQL 5.7].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Compatability_Dictionaries = Requirement(
        name='RQ.SRS008.AES.Functions.Compatability.Dictionaries',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support encryption and decryption of data accessed on remote\n'
        '[MySQL] servers using [MySQL Dictionary].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Compatability_Engine_Database_MySQL = Requirement(
        name='RQ.SRS008.AES.Functions.Compatability.Engine.Database.MySQL',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Database Engine],\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Compatability_Engine_Table_MySQL = Requirement(
        name='RQ.SRS008.AES.Functions.Compatability.Engine.Table.MySQL',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Table Engine].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Compatability_TableFunction_MySQL = Requirement(
        name='RQ.SRS008.AES.Functions.Compatability.TableFunction.MySQL',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Table Function].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_DifferentModes = Requirement(
        name='RQ.SRS008.AES.Functions.DifferentModes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL allow different modes to be supported in a single SQL statement\n'
        'using explicit function parameters.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_DataFromMultipleSources = Requirement(
        name='RQ.SRS008.AES.Functions.DataFromMultipleSources',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support handling encryption and decryption of data from multiple sources\n'
        'in the `SELECT` statement, including [ClickHouse] [MergeTree] table as well as [MySQL Dictionary],\n'
        '[MySQL Database Engine], [MySQL Table Engine], and [MySQL Table Function]\n'
        'with possibly different encryption schemes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_SuppressOutputOfSensitiveValues = Requirement(
        name='RQ.SRS008.AES.Functions.SuppressOutputOfSensitiveValues',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL suppress output of [AES] `string` and `key` parameters to the system log,\n'
        'error log, and `query_log` table to prevent leakage of sensitive values.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_InvalidParameters = Requirement(
        name='RQ.SRS008.AES.Functions.InvalidParameters',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when parameters are invalid.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Mismatched_Key = Requirement(
        name='RQ.SRS008.AES.Functions.Mismatched.Key',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return garbage for mismatched keys.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Mismatched_IV = Requirement(
        name='RQ.SRS008.AES.Functions.Mismatched.IV',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return garbage for mismatched initialization vector for the modes that use it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Mismatched_AAD = Requirement(
        name='RQ.SRS008.AES.Functions.Mismatched.AAD',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return garbage for mismatched additional authentication data for the modes that use it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Mismatched_Mode = Requirement(
        name='RQ.SRS008.AES.Functions.Mismatched.Mode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error or garbage for mismatched mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Check_Performance = Requirement(
        name='RQ.SRS008.AES.Functions.Check.Performance',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'Performance of [AES] encryption functions SHALL be measured.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Function_Check_Performance_BestCase = Requirement(
        name='RQ.SRS008.AES.Function.Check.Performance.BestCase',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'Performance of [AES] encryption functions SHALL be checked for the best case\n'
        'scenario where there is one key, one initialization vector, and one large stream of data.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Function_Check_Performance_WorstCase = Requirement(
        name='RQ.SRS008.AES.Function.Check.Performance.WorstCase',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'Performance of [AES] encryption functions SHALL be checked for the worst case\n'
        'where there are `N` keys, `N` initialization vectors and `N` very small streams of data.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Check_Compression = Requirement(
        name='RQ.SRS008.AES.Functions.Check.Compression',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'Effect of [AES] encryption on column compression SHALL be measured.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Functions_Check_Compression_LowCardinality = Requirement(
        name='RQ.SRS008.AES.Functions.Check.Compression.LowCardinality',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'Effect of [AES] encryption on the compression of a column with [LowCardinality] data type\n'
        'SHALL be measured.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes_encrypt` function to encrypt data using [AES].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Syntax = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `aes_encrypt` function\n'
        '\n'
        '```sql\n'
        'aes_encrypt(plaintext, key, mode, [iv, aad])\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_NIST_TestVectors = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.NIST.TestVectors',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] `aes_encrypt` function output SHALL produce output that matches [NIST test vectors].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_PlainText = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `plaintext` accepting any data type as\n'
        'the first parameter to the `aes_encrypt` function that SHALL specify the data to be encrypted.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Key = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Key',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `key` with `String` or `FixedString` data types\n'
        'as the second parameter to the `aes_encrypt` function that SHALL specify the encryption key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter\n'
        'to the `aes_encrypt` function that SHALL specify encryption key length and block encryption mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_ValuesFormat = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.ValuesFormat',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter\n'
        'of the `aes_encrypt` function where\n'
        'the `key_length` SHALL specifies the length of the key and SHALL accept\n'
        '`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption\n'
        'mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB] as well as\n'
        '[CTR] and [GCM] as the values. For example, `aes-256-ofb`.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_Invalid = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_encrypt`\n'
        'function is not valid with the exception where such a mode is supported by the underlying\n'
        '[OpenSSL] implementation.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_ECB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_ECB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_ECB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_CBC = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_CBC = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_CBC = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB1 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB1 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB1 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB8 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB8 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB8 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB128 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB128 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB128 = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_OFB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_OFB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_OFB = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_GCM = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-gcm` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 128 bit key.\n'
        'An `AEAD` 16-byte tag is appended to the resulting ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_GCM = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-gcm` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 192 bit key.\n'
        'An `AEAD` 16-byte tag is appended to the resulting ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_GCM = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-gcm` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 256 bit key.\n'
        'An `AEAD` 16-byte tag is appended to the resulting ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_128_CTR = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ctr` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_192_CTR = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ctr` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_Mode_Value_AES_256_CTR = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ctr` as the value for the `mode` parameter of the `aes_encrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_InitializationVector = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.InitializationVector',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth\n'
        'parameter to the `aes_encrypt` function that SHALL specify the initialization vector for block modes that require\n'
        'it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_AdditionalAuthenticatedData = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.AdditionalAuthenticatedData',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aad` with `String` or `FixedString` data types as the optional fifth\n'
        'parameter to the `aes_encrypt` function that SHALL specify the additional authenticated data\n'
        'for block modes that require it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Parameters_ReturnValue = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Parameters.ReturnValue',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return the encrypted value of the data\n'
        'using `String` data type as the result of `aes_encrypt` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_Key_Length_InvalidLengthError = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.Key.Length.InvalidLengthError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `key` length is not exact for the `aes_encrypt` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_InitializationVector_Length_InvalidLengthError = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.InitializationVector.Length.InvalidLengthError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` length is specified and not of the exact size for the `aes_encrypt` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_InitializationVector_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.InitializationVector.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_encrypt` function for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `aad` is specified for the `aes_encrypt` function for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AdditionalAuthenticationData_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not limit the size of the `aad` parameter passed to the `aes_encrypt` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ecb` and `key` is not 16 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ecb` and `key` is not 24 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ecb` and `key` is not 32 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cbc` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cbc` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cbc` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb1` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb1` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb1` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb8` and `key` is not 16 bytes\n'
        'and if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb8` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb8` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb128` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb128` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb128` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ofb` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ofb` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ofb` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-gcm` and `key` is not 16 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-gcm` and `key` is not 24 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-gcm` and `key` is not 32 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_128_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ctr` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_192_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ctr` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Encrypt_Function_AES_256_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Encrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ctr` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes_decrypt` function to decrypt data using [AES].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Syntax = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `aes_decrypt` function\n'
        '\n'
        '```sql\n'
        'aes_decrypt(ciphertext, key, mode, [iv, aad])\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_CipherText = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.CipherText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `ciphertext` accepting `FixedString` or `String` data types as\n'
        'the first parameter to the `aes_decrypt` function that SHALL specify the data to be decrypted.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Key = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Key',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `key` with `String` or `FixedString` data types\n'
        'as the second parameter to the `aes_decrypt` function that SHALL specify the encryption key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter\n'
        'to the `aes_decrypt` function that SHALL specify encryption key length and block encryption mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_ValuesFormat = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.ValuesFormat',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter\n'
        'of the `aes_decrypt` function where\n'
        'the `key_length` SHALL specifies the length of the key and SHALL accept\n'
        '`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption\n'
        'mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB] as well as\n'
        '[CTR] and [GCM] as the values. For example, `aes-256-ofb`.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_Invalid = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_decrypt`\n'
        'function is not valid with the exception where such a mode is supported by the underlying\n'
        '[OpenSSL] implementation.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_ECB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_ECB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_ECB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_CBC = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_CBC = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_CBC = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB1 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB1 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB1 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB8 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB8 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB8 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB128 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB128 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB128 = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_OFB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_OFB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_OFB = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_GCM = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-gcm` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 128 bit key.\n'
        'An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_GCM = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-gcm` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 192 bit key.\n'
        'An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_GCM = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-gcm` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [GCM] block mode encryption with a 256 bit key.\n'
        'An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to\n'
        'the [RFC5116].\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_128_CTR = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ctr` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_192_CTR = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ctr` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_Mode_Value_AES_256_CTR = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ctr` as the value for the `mode` parameter of the `aes_decrypt` function\n'
        'and [AES] algorithm SHALL use the [CTR] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_InitializationVector = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.InitializationVector',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth\n'
        'parameter to the `aes_decrypt` function that SHALL specify the initialization vector for block modes that require\n'
        'it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_AdditionalAuthenticatedData = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.AdditionalAuthenticatedData',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aad` with `String` or `FixedString` data types as the optional fifth\n'
        'parameter to the `aes_decrypt` function that SHALL specify the additional authenticated data\n'
        'for block modes that require it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Parameters_ReturnValue = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Parameters.ReturnValue',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return the decrypted value of the data\n'
        'using `String` data type as the result of `aes_decrypt` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_Key_Length_InvalidLengthError = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.Key.Length.InvalidLengthError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `key` length is not exact for the `aes_decrypt` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_InitializationVector_Length_InvalidLengthError = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.InitializationVector.Length.InvalidLengthError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` is speficified and the length is not exact for the `aes_decrypt` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_InitializationVector_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.InitializationVector.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_decrypt` function\n'
        'for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `aad` is specified for the `aes_decrypt` function\n'
        'for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AdditionalAuthenticationData_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not limit the size of the `aad` parameter passed to the `aes_decrypt` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ecb` and `key` is not 16 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ecb` and `key` is not 24 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ecb` and `key` is not 32 bytes\n'
        'or `iv` or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cbc` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cbc` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cbc` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb1` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb1` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb1` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb8` and `key` is not 16 bytes\n'
        'and if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb8` and `key` is not 24 bytes\n'
        'or `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb8` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb128` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb128` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb128` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ofb` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ofb` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ofb` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes or `aad` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-gcm` and `key` is not 16 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-gcm` and `key` is not 24 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_GCM_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-gcm` and `key` is not 32 bytes\n'
        'or `iv` is not specified or is less than 8 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_128_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ctr` and `key` is not 16 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_192_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ctr` and `key` is not 24 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_Decrypt_Function_AES_256_CTR_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.Decrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ctr` and `key` is not 32 bytes\n'
        'or if specified `iv` is not 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes_encrypt_mysql` function to encrypt data using [AES].\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Syntax = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `aes_encrypt_mysql` function\n'
        '\n'
        '```sql\n'
        'aes_encrypt_mysql(plaintext, key, mode, [iv])\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_PlainText = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `plaintext` accepting any data type as\n'
        'the first parameter to the `aes_encrypt_mysql` function that SHALL specify the data to be encrypted.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Key = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Key',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `key` with `String` or `FixedString` data types\n'
        'as the second parameter to the `aes_encrypt_mysql` function that SHALL specify the encryption key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter\n'
        'to the `aes_encrypt_mysql` function that SHALL specify encryption key length and block encryption mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_ValuesFormat = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.ValuesFormat',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter\n'
        'of the `aes_encrypt_mysql` function where\n'
        'the `key_length` SHALL specifies the length of the key and SHALL accept\n'
        '`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption\n'
        'mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB]. For example, `aes-256-ofb`.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_Invalid = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_encrypt_mysql`\n'
        'function is not valid with the exception where such a mode is supported by the underlying\n'
        '[OpenSSL] implementation.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-128-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-192-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-256-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_128_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-128-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_192_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-192-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_Mode_Value_AES_256_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-256-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_InitializationVector = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.InitializationVector',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth\n'
        'parameter to the `aes_encrypt_mysql` function that SHALL specify the initialization vector for block modes that require\n'
        'it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Parameters_ReturnValue = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.ReturnValue',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return the encrypted value of the data\n'
        'using `String` data type as the result of `aes_encrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Key_Length_TooShortError = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooShortError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `key` length is less than the minimum for the `aes_encrypt_mysql`\n'
        'function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_Key_Length_TooLong = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooLong',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use folding algorithm specified below if the `key` length is longer than required\n'
        'for the `aes_encrypt_mysql` function for a given block mode.\n'
        '\n'
        '```python\n'
        'def fold_key(key, cipher_key_size):\n'
        '    key = list(key) if not isinstance(key, (list, tuple)) else key\n'
        '\t  folded_key = key[:cipher_key_size]\n'
        '\t  for i in range(cipher_key_size, len(key)):\n'
        '\t\t    print(i % cipher_key_size, i)\n'
        '\t\t    folded_key[i % cipher_key_size] ^= key[i]\n'
        '\t  return folded_key\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_Length_TooShortError = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooShortError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` length is specified and is less than the minimum\n'
        'that is required for the `aes_encrypt_mysql` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_Length_TooLong = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooLong',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the first `N` bytes that are required if the `iv` is specified and\n'
        'its length is longer than required for the `aes_encrypt_mysql` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_InitializationVector_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_encrypt_mysql`\n'
        'function for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-ecb` and `key` is less than 16 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-ecb` and `key` is less than 24 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-ecb` and `key` is less than 32 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cbc` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cbc` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cbc` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb1` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb1` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb1` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb8` and `key` is less than 16 bytes\n'
        'and if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb8` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb8` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb128` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb128` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb128` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_128_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-ofb` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_192_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-ofb` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Encrypt_Function_AES_256_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-ofb` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes_decrypt_mysql` function to decrypt data using [AES].\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Syntax = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `aes_decrypt_mysql` function\n'
        '\n'
        '```sql\n'
        'aes_decrypt_mysql(ciphertext, key, mode, [iv])\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_CipherText = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.CipherText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `ciphertext` accepting any data type as\n'
        'the first parameter to the `aes_decrypt_mysql` function that SHALL specify the data to be decrypted.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Key = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Key',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `key` with `String` or `FixedString` data types\n'
        'as the second parameter to the `aes_decrypt_mysql` function that SHALL specify the encryption key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter\n'
        'to the `aes_decrypt_mysql` function that SHALL specify encryption key length and block encryption mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_ValuesFormat = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.ValuesFormat',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter\n'
        'of the `aes_decrypt_mysql` function where\n'
        'the `key_length` SHALL specifies the length of the key and SHALL accept\n'
        '`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption\n'
        'mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB]. For example, `aes-256-ofb`.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_Invalid = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_decrypt_mysql`\n'
        'function is not valid with the exception where such a mode is supported by the underlying\n'
        '[OpenSSL] implementation.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_ECB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_CBC = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB1 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB8 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_CFB128 = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_OFB = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function\n'
        'and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-128-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-192-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_GCM_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-256-gcm` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_128_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-128-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_192_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-192-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_Mode_Value_AES_256_CTR_Error = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if `aes-256-ctr` is specified as the value for the `mode` parameter of the\n'
        '`aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_InitializationVector = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.InitializationVector',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth\n'
        'parameter to the `aes_decrypt_mysql` function that SHALL specify the initialization vector for block modes that require\n'
        'it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Parameters_ReturnValue = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.ReturnValue',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return the decrypted value of the data\n'
        'using `String` data type as the result of `aes_decrypt_mysql` function.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Key_Length_TooShortError = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooShortError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `key` length is less than the minimum for the `aes_decrypt_mysql`\n'
        'function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_Key_Length_TooLong = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooLong',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use folding algorithm specified below if the `key` length is longer than required\n'
        'for the `aes_decrypt_mysql` function for a given block mode.\n'
        '\n'
        '```python\n'
        'def fold_key(key, cipher_key_size):\n'
        '    key = list(key) if not isinstance(key, (list, tuple)) else key\n'
        '\t  folded_key = key[:cipher_key_size]\n'
        '\t  for i in range(cipher_key_size, len(key)):\n'
        '\t\t    print(i % cipher_key_size, i)\n'
        '\t\t    folded_key[i % cipher_key_size] ^= key[i]\n'
        '\t  return folded_key\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_Length_TooShortError = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooShortError',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` length is specified and is less than the minimum\n'
        'that is required for the `aes_decrypt_mysql` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_Length_TooLong = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooLong',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the first `N` bytes that are required if the `iv` is specified and\n'
        'its length is longer than required for the `aes_decrypt_mysql` function for a given block mode.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_InitializationVector_NotValidForMode = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.NotValidForMode',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_decrypt_mysql`\n'
        'function for a mode that does not need it.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-ecb` and `key` is less than 16 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-ecb` and `key` is less than 24 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_ECB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-ecb` and `key` is less than 32 bytes\n'
        'or `iv` is specified.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cbc` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cbc` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_CBC_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cbc` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb1` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb1` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_CFB1_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb1` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb8` and `key` is less than 16 bytes\n'
        'and if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb8` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_CFB8_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb8` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb128` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb128` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_CFB128_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb128` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_128_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-ofb` and `key` is less than 16 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_192_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-ofb` and `key` is less than 24 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )

RQ_SRS008_AES_MySQL_Decrypt_Function_AES_256_OFB_KeyAndInitializationVector_Length = Requirement(
        name='RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-ofb` and `key` is less than 32 bytes\n'
        'or if specified `iv` is less than 16 bytes.\n'
        ),
        link=None
    )
