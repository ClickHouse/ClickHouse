#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U_ADMIN="u_admin_${CLICKHOUSE_DATABASE}"
U_ANALYST="u_analyst_${CLICKHOUSE_DATABASE}"
DICT_TBL="${CLICKHOUSE_DATABASE}.dict_mask_rules"
DICT_NAME="${CLICKHOUSE_DATABASE}.mask_rules_dict"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.v_sec_def_alias;" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${DICT_NAME};" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DICT_TBL}, ${CLICKHOUSE_DATABASE}.logs_masking;" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_ADMIN}, ${U_ANALYST};" >/dev/null 2>&1 || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -n -q "
DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.v_sec_def_alias;
DROP DICTIONARY IF EXISTS ${DICT_NAME};
DROP TABLE IF EXISTS ${DICT_TBL}, ${CLICKHOUSE_DATABASE}.logs_masking;
DROP USER IF EXISTS ${U_ADMIN}, ${U_ANALYST};

CREATE USER ${U_ADMIN};
CREATE USER ${U_ANALYST};

CREATE TABLE ${DICT_TBL}
(
    user String,
    attr_key String,
    blocked UInt8
)
ENGINE = MergeTree
ORDER BY (user, attr_key);

INSERT INTO ${DICT_TBL} VALUES
    ('${U_ANALYST}', 'ssn', 1),
    ('${U_ANALYST}', 'secret_tag', 1);

CREATE DICTIONARY ${DICT_NAME}
(
    user String,
    attr_key String,
    blocked UInt8
)
PRIMARY KEY user, attr_key
SOURCE(CLICKHOUSE(TABLE 'dict_mask_rules' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 0);

CREATE TABLE ${CLICKHOUSE_DATABASE}.logs_masking
(
    ts UInt32,
    secret_key_col String,
    public_key_col String,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service'],
    svc_alias String ALIAS raw_attrs['service'],
    attrs_empty Map(String, String) ALIAS CAST(map(), 'Map(String, String)'),
    attrs_static Map(String, String) ALIAS mapApply(
        (k, v) -> (k, multiIf(
            currentUser() = '${U_ADMIN}', v,
            k = 'ssn', '***',
            k = 'email', concat('***@', splitByChar('@', v)[2]),
            v
        )),
        mapFilter(
            (k, v) -> (currentUser() = '${U_ADMIN}' OR (k != 'internal_token' AND NOT startsWith(v, 'secret:'))),
            raw_attrs
        )
    ),
    attrs_dict Map(String, String) ALIAS mapFilter(
        (k, v) -> (dictGetOrDefault('${DICT_NAME}', 'blocked', (currentUser(), k), toUInt8(0)) = 0),
        raw_attrs
    )
)
ENGINE = MergeTree
ORDER BY (__mat_service, ts);

INSERT INTO ${CLICKHOUSE_DATABASE}.logs_masking VALUES
    (1, 'service', 'service', {'service': 'checkout', 'ssn': '111-11', 'email': 'alice@acme.com', 'internal_token': 'tok1', 'msg': 'secret:payload', 'secret_tag': 't1'}),
    (2, 'ssn',     'email',   {'service': 'auth',     'ssn': '222-22', 'email': 'bob@acme.com',   'internal_token': 'tok2', 'msg': 'ok_msg',         'secret_tag': 't2'});

GRANT SELECT(ts, public_key_col, svc_alias, attrs_empty, attrs_static, attrs_dict) ON ${CLICKHOUSE_DATABASE}.logs_masking TO ${U_ADMIN};
GRANT SELECT(ts, public_key_col, svc_alias, attrs_empty, attrs_static, attrs_dict) ON ${CLICKHOUSE_DATABASE}.logs_masking TO ${U_ANALYST};
GRANT dictGet ON ${DICT_NAME} TO ${U_ADMIN};
GRANT dictGet ON ${DICT_NAME} TO ${U_ANALYST};

CREATE VIEW ${CLICKHOUSE_DATABASE}.v_sec_def_alias DEFINER = ${U_ADMIN} SQL SECURITY DEFINER AS
SELECT ts, attrs_static['ssn'] AS ssn_val FROM ${CLICKHOUSE_DATABASE}.logs_masking;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_sec_def_alias TO ${U_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_sec_def_alias TO ${U_ANALYST};
"

echo "=== 1. RBAC blocks direct access to raw_attrs, __mat_service, and ungranted key columns ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" -q "SELECT raw_attrs FROM ${CLICKHOUSE_DATABASE}.logs_masking" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" -q "SELECT __mat_service FROM ${CLICKHOUSE_DATABASE}.logs_masking" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "SELECT attrs_static[secret_key_col] FROM ${CLICKHOUSE_DATABASE}.logs_masking" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "SELECT attrs_empty[secret_key_col] FROM ${CLICKHOUSE_DATABASE}.logs_masking" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "=== 2. Static & value-based masking via pushdown + MATERIALIZED under RBAC ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_static['service'], attrs_static['ssn'], attrs_static['email'], attrs_static['missing'], attrs_static['internal_token'], attrs_static['msg']
FROM ${CLICKHOUSE_DATABASE}.logs_masking
WHERE attrs_static['service'] IN ('checkout', 'auth')
ORDER BY ts;
SELECT ts, attrs_static['service'] FROM ${CLICKHOUSE_DATABASE}.logs_masking PREWHERE attrs_static['service'] = 'checkout' ORDER BY ts;
SELECT ts, attrs_static[public_key_col] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
SELECT ts, length(attrs_static['service']), empty(svc_alias) FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
SELECT ts, k, has(attrs_static, k) FROM ${CLICKHOUSE_DATABASE}.logs_masking ARRAY JOIN ['service', 'internal_token'] AS k ORDER BY ts, k;
SELECT count() FROM ${CLICKHOUSE_DATABASE}.logs_masking WHERE indexHint(attrs_static['service'] = 'checkout');
"

echo "=== 3. Whole-map SELECT cannot bypass masking ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 -q "
SELECT ts, mapSort(attrs_static) FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"

echo "=== 4. Admin sees unmasked values ==="
${CLICKHOUSE_CLIENT} --user "${U_ADMIN}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -q "
SELECT ts, attrs_static['service'], attrs_static['ssn'], attrs_static['email'], attrs_static['internal_token'], attrs_static['msg']
FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"

echo "=== 5. Dictionary-driven masking + EXPLAIN SYNTAX run_query_tree_passes = 1 + EXPLAIN actions = 1 ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -n -q "
SELECT ts, attrs_dict['service'], attrs_dict['ssn'], attrs_dict['secret_tag'] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN actions = 1 SELECT attrs_dict['service'] FROM ${CLICKHOUSE_DATABASE}.logs_masking" | grep -q "__mat_service" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN actions = 1 SELECT attrs_dict['service'] FROM ${CLICKHOUSE_DATABASE}.logs_masking" | grep -q "raw_attrs" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --enable_parallel_replicas=0 -q "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT attrs_static['service'] FROM ${CLICKHOUSE_DATABASE}.logs_masking" | grep -E "raw_attrs|__mat_service" | wc -l

echo "=== 6. Live dictionary reload updates masking with zero table DDL ==="
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DICT_TBL} VALUES ('${U_ANALYST}', 'service', 1);"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD DICTIONARY ${DICT_NAME};"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 -q "
SELECT ts, attrs_dict['service'], attrs_dict['ssn'] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"

echo "=== 7. QueryResultCache: unrelated queries cache cleanly, non-deterministic ALIAS does not leak ==="
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --use_query_cache=1 -q "
SELECT count() FROM ${CLICKHOUSE_DATABASE}.logs_masking;
"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --optimize_map_materialized_columns=1 --use_query_cache=1 -q "
SELECT attrs_static['ssn'] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
" 2>&1 | grep -m1 -o "QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS"
${CLICKHOUSE_CLIENT} --user "${U_ADMIN}" --optimize_map_alias_pushdown=1 --use_query_cache=1 -q "
SELECT ts, ssn_val FROM ${CLICKHOUSE_DATABASE}.v_sec_def_alias ORDER BY ts;
" 2>&1 | grep -m1 -o "QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS"

${CLICKHOUSE_CLIENT} --user "${U_ADMIN}" --optimize_map_alias_pushdown=1 --use_query_cache=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, attrs_static['ssn'] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --use_query_cache=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, attrs_static['ssn'] FROM ${CLICKHOUSE_DATABASE}.logs_masking ORDER BY ts;
"
${CLICKHOUSE_CLIENT} --user "${U_ADMIN}" --optimize_map_alias_pushdown=1 --query_cache_for_subqueries=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, s FROM (SELECT ts, attrs_static['ssn'] AS s FROM ${CLICKHOUSE_DATABASE}.logs_masking) ORDER BY ts SETTINGS query_cache_for_subqueries = 1, use_query_cache = 1;
"
${CLICKHOUSE_CLIENT} --user "${U_ANALYST}" --optimize_map_alias_pushdown=1 --query_cache_for_subqueries=1 --query_cache_nondeterministic_function_handling=save --query_cache_share_between_users=1 -q "
SELECT ts, s FROM (SELECT ts, attrs_static['ssn'] AS s FROM ${CLICKHOUSE_DATABASE}.logs_masking) ORDER BY ts SETTINGS query_cache_for_subqueries = 1, use_query_cache = 1;
"
