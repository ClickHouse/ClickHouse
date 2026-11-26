#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U_ADMIN="u_adv_admin_${CLICKHOUSE_DATABASE}"
U_ANALYST="u_adv_analyst_${CLICKHOUSE_DATABASE}"
U_RESTRICTED="u_adv_restr_${CLICKHOUSE_DATABASE}"
R_RESTRICTED="r_adv_restr_${CLICKHOUSE_DATABASE}"
R_OPEN="r_adv_open_${CLICKHOUSE_DATABASE}"

USER_POLICY_TBL="${CLICKHOUSE_DATABASE}.adv_user_policy_tbl"
USER_POLICY_DICT="${CLICKHOUSE_DATABASE}.adv_user_policy_dict"
ACTION_TBL="${CLICKHOUSE_DATABASE}.adv_action_policy_tbl"
ACTION_DICT="${CLICKHOUSE_DATABASE}.adv_action_policy_dict"
ROLE_TBL="${CLICKHOUSE_DATABASE}.adv_role_block_tbl"
ROLE_DICT="${CLICKHOUSE_DATABASE}.adv_role_block_dict"
LOGS_TBL="${CLICKHOUSE_DATABASE}.adv_logs"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${USER_POLICY_DICT}, ${ACTION_DICT}, ${ROLE_DICT};" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${USER_POLICY_TBL}, ${ACTION_TBL}, ${ROLE_TBL}, ${LOGS_TBL};" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${R_RESTRICTED}, ${R_OPEN};" >/dev/null 2>&1 || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -n -q "
DROP DICTIONARY IF EXISTS ${USER_POLICY_DICT}, ${ACTION_DICT}, ${ROLE_DICT};
DROP TABLE IF EXISTS ${USER_POLICY_TBL}, ${ACTION_TBL}, ${ROLE_TBL}, ${LOGS_TBL};
DROP USER IF EXISTS ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};
DROP ROLE IF EXISTS ${R_RESTRICTED}, ${R_OPEN};

CREATE ROLE ${R_RESTRICTED}, ${R_OPEN};
CREATE USER ${U_ADMIN} DEFAULT ROLE ${R_OPEN};
CREATE USER ${U_ANALYST};
CREATE USER ${U_RESTRICTED} DEFAULT ROLE ${R_RESTRICTED};

-- Scenario A & C1 backing table: per-user allowlist, denylist, and blocked prefix arrays
CREATE TABLE ${USER_POLICY_TBL}
(
    user String,
    allowed_attrs Array(String),
    blocked_attrs Array(String),
    blocked_prefixes Array(String)
)
ENGINE = MergeTree
ORDER BY user;

INSERT INTO ${USER_POLICY_TBL} VALUES
    ('${U_ANALYST}', ['service', 'http.url', 'client_ip'], ['ssn', 'credit_card'], ['pii.', 'internal.']);

CREATE DICTIONARY ${USER_POLICY_DICT}
(
    user String,
    allowed_attrs Array(String),
    blocked_attrs Array(String),
    blocked_prefixes Array(String)
)
PRIMARY KEY user
SOURCE(CLICKHOUSE(TABLE 'adv_user_policy_tbl' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 0);

-- Scenario B backing table: per-(user, key) action ('allow', 'drop', 'redact', 'hash', 'hmac', 'ip_mask', 'url_strip', 'email_mask')
CREATE TABLE ${ACTION_TBL}
(
    user String,
    attr_key String,
    action String
)
ENGINE = MergeTree
ORDER BY (user, attr_key);

INSERT INTO ${ACTION_TBL} VALUES
    ('${U_ANALYST}', 'ssn', 'hash'),
    ('${U_ANALYST}', 'email', 'hmac'),
    ('${U_ANALYST}', 'http.url', 'url_strip'),
    ('${U_ANALYST}', 'client_ip', 'ip_mask'),
    ('${U_ANALYST}', 'secret_note', 'drop'),
    ('${U_ANALYST}', 'token', 'redact');

CREATE DICTIONARY ${ACTION_DICT}
(
    user String,
    attr_key String,
    action String
)
PRIMARY KEY user, attr_key
SOURCE(CLICKHOUSE(TABLE 'adv_action_policy_tbl' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 0);

-- Scenario C2 backing table: role-based blocked keys
CREATE TABLE ${ROLE_TBL}
(
    role String,
    attr_key String,
    blocked UInt8
)
ENGINE = MergeTree
ORDER BY (role, attr_key);

INSERT INTO ${ROLE_TBL} VALUES
    ('${R_RESTRICTED}', 'ssn', 1),
    ('${R_RESTRICTED}', 'secret_note', 1);

CREATE DICTIONARY ${ROLE_DICT}
(
    role String,
    attr_key String,
    blocked UInt8
)
PRIMARY KEY role, attr_key
SOURCE(CLICKHOUSE(TABLE 'adv_role_block_tbl' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 0);

CREATE TABLE ${LOGS_TBL}
(
    ts UInt32,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service'],
    -- Scenario A1: Runtime-adjustable attribute allowlist per user from dictionary
    attrs_allow Map(String, String) ALIAS mapFilter(
        (k, v) -> has(dictGetOrDefault('${USER_POLICY_DICT}', 'allowed_attrs', currentUser(), CAST([], 'Array(String)')), k),
        raw_attrs
    ),
    -- Scenario A2: Array(String) denylist per user from dictionary
    attrs_arr_deny Map(String, String) ALIAS mapFilter(
        (k, v) -> NOT has(dictGetOrDefault('${USER_POLICY_DICT}', 'blocked_attrs', currentUser(), CAST([], 'Array(String)')), k),
        raw_attrs
    ),
    -- Scenario B: Multi-action redaction policy using native ClickHouse sanitization functions
    attrs_action Map(String, String) ALIAS mapApply(
        (k, v) -> (k, multiIf(
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'redact', '***',
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'hash', substring(hex(SHA256(v)), 1, 12),
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'hmac', substring(hex(HMAC('sha256', v, 'audit_hmac_key')), 1, 12),
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'ip_mask', IPv4NumToStringClassC(toIPv4OrZero(v)),
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'url_strip', cutURLParameter(v, 'token'),
            dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') = 'email_mask', concat('***@', splitByChar('@', v)[2]),
            v
        )),
        mapFilter(
            (k, v) -> dictGetOrDefault('${ACTION_DICT}', 'action', (currentUser(), k), 'allow') != 'drop',
            raw_attrs
        )
    ),
    -- Scenario C1: Nested lambda matching Array(String) prefix list per user
    attrs_prefix Map(String, String) ALIAS mapFilter(
        (k, v) -> NOT arrayExists(
            p -> startsWith(k, p),
            dictGetOrDefault('${USER_POLICY_DICT}', 'blocked_prefixes', currentUser(), CAST([], 'Array(String)'))
        ),
        raw_attrs
    ),
    -- Scenario C2: Nested lambda checking role-based dictionary via currentRoles
    attrs_role Map(String, String) ALIAS mapFilter(
        (k, v) -> NOT arrayExists(
            r -> dictHas('${ROLE_DICT}', (r, k)),
            currentRoles()
        ),
        raw_attrs
    )
)
ENGINE = MergeTree
ORDER BY (__mat_service, ts)
SETTINGS index_granularity = 1;

INSERT INTO ${LOGS_TBL} VALUES
    (1, {
        'service': 'checkout',
        'http.url': 'https://shop.local/pay?item=book&token=sec1&ok=1',
        'client_ip': '10.20.30.44',
        'ssn': '111-11',
        'credit_card': '4111',
        'email': 'alice@acme.com',
        'secret_note': 'top_secret',
        'token': 'tok_123',
        'pii.phone': '555-0100',
        'internal.host': 'db-01'
    }),
    (2, {
        'service': 'auth',
        'http.url': 'https://shop.local/login?token=sec2&next=/home',
        'client_ip': '192.168.55.99',
        'ssn': '222-22',
        'credit_card': '5500',
        'email': 'bob@acme.com',
        'secret_note': 'hidden',
        'token': 'tok_456',
        'pii.phone': '555-0200',
        'internal.host': 'db-02'
    });

GRANT SELECT(ts, attrs_allow, attrs_arr_deny, attrs_action, attrs_prefix, attrs_role) ON ${LOGS_TBL} TO ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};
GRANT dictGet ON ${USER_POLICY_DICT} TO ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};
GRANT dictGet ON ${ACTION_DICT} TO ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};
GRANT dictGet ON ${ROLE_DICT} TO ${U_ADMIN}, ${U_ANALYST}, ${U_RESTRICTED};
"

echo "=== A1. Runtime-adjustable attribute allowlist per user + live dict update ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_allow['service'], attrs_allow['client_ip'], attrs_allow['email'], attrs_allow['ssn'], has(attrs_allow, 'email')
FROM ${LOGS_TBL} WHERE attrs_allow['service'] = 'checkout' ORDER BY ts;
SELECT ts, mapSort(attrs_allow) FROM ${LOGS_TBL} WHERE ts = 1;
"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${USER_POLICY_TBL} UPDATE allowed_attrs = ['service', 'email'] WHERE user = '${U_ANALYST}' SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${USER_POLICY_DICT};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -q "
SELECT ts, attrs_allow['service'], attrs_allow['email'], attrs_allow['http.url'] FROM ${LOGS_TBL} ORDER BY ts;
"

echo "=== A2. Array(String) denylist per user + EXPLAIN uses __mat_service ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_arr_deny['service'], attrs_arr_deny['ssn'], attrs_arr_deny['credit_card'], attrs_arr_deny['email'], has(attrs_arr_deny, 'ssn')
FROM ${LOGS_TBL} WHERE attrs_arr_deny['service'] = 'checkout' ORDER BY ts;
"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN actions = 1 SELECT attrs_arr_deny['service'] FROM ${LOGS_TBL}" | grep -q "__mat_service" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN actions = 1 SELECT attrs_arr_deny['service'] FROM ${LOGS_TBL}" | grep -q "raw_attrs" && echo 1 || echo 0

echo "=== B1. Multi-action redaction modes (SHA256, HMAC, cutURLParameter, IPv4NumToStringClassC, drop, redact, allow) ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_action['service'], attrs_action['ssn'], attrs_action['email'], attrs_action['http.url'], attrs_action['client_ip'], attrs_action['secret_note'], attrs_action['token'], attrs_action['missing'], has(attrs_action, 'secret_note')
FROM ${LOGS_TBL} ORDER BY ts;
SELECT ts, mapSort(attrs_action) FROM ${LOGS_TBL} WHERE ts = 1;
"

echo "=== B2. Live dictionary action transition for email (hmac -> email_mask -> hash -> drop) ==="
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${ACTION_TBL} UPDATE action = 'email_mask' WHERE user = '${U_ANALYST}' AND attr_key = 'email' SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${ACTION_DICT};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "
SELECT ts, attrs_action['email'] FROM ${LOGS_TBL} ORDER BY ts;
"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${ACTION_TBL} UPDATE action = 'hash' WHERE user = '${U_ANALYST}' AND attr_key = 'email' SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${ACTION_DICT};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "
SELECT ts, attrs_action['email'] FROM ${LOGS_TBL} ORDER BY ts;
"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${ACTION_TBL} UPDATE action = 'drop' WHERE user = '${U_ANALYST}' AND attr_key = 'email' SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${ACTION_DICT};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "
SELECT ts, attrs_action['email'], has(attrs_action, 'email') FROM ${LOGS_TBL} ORDER BY ts;
"

echo "=== C1. Nested lambda prefix denylist + live reload adding http. prefix ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_prefix['service'], attrs_prefix['http.url'], attrs_prefix['pii.phone'], attrs_prefix['internal.host']
FROM ${LOGS_TBL} ORDER BY ts;
SELECT ts, mapSort(attrs_prefix) FROM ${LOGS_TBL} WHERE ts = 1;
"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${USER_POLICY_TBL} UPDATE blocked_prefixes = ['pii.', 'internal.', 'http.'] WHERE user = '${U_ANALYST}' SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${USER_POLICY_DICT};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -q "
SELECT ts, attrs_prefix['service'], attrs_prefix['http.url'] FROM ${LOGS_TBL} ORDER BY ts;
"

echo "=== C2. Nested lambda role-based dictionary + QueryResultCache non-leakage ==="
${CLICKHOUSE_CLIENT} --user "${U_RESTRICTED}" -q "SELECT raw_attrs FROM ${LOGS_TBL}" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${U_ADMIN}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --use_query_cache=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, attrs_role['service'], attrs_role['ssn'], attrs_role['secret_note'] FROM ${LOGS_TBL} ORDER BY ts;
"
${CLICKHOUSE_CLIENT} --user "${U_RESTRICTED}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --use_query_cache=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, attrs_role['service'], attrs_role['ssn'], attrs_role['secret_note'] FROM ${LOGS_TBL} ORDER BY ts;
"

echo "=== D. Primary-key condition extraction across allowlist & nested-lambda policies ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN indexes = 1 SELECT count() FROM ${LOGS_TBL} WHERE attrs_allow['service'] = 'checkout' FORMAT TSVRaw" | grep -qF "Condition: (__mat_service in ['checkout', 'checkout'])" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN indexes = 1 SELECT count() FROM ${LOGS_TBL} WHERE attrs_prefix['service'] = 'checkout' FORMAT TSVRaw" | grep -qF "Condition: (__mat_service in ['checkout', 'checkout'])" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user "${U_RESTRICTED}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN indexes = 1 SELECT count() FROM ${LOGS_TBL} WHERE attrs_role['service'] = 'checkout' FORMAT TSVRaw" | grep -qF "Condition: (__mat_service in ['checkout', 'checkout'])" && echo 1 || echo 0
