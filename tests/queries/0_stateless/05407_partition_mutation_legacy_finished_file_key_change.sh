#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `DETACH TABLE` is used to make `loadMutations` read the mutation files again.
#
# Upgrade regression: an older server version applied an `IN PARTITION <value>` mutation of `MergeTree`
# to all parts and kept the original literal in `mutation_*.txt` (`finished_mutations_to_keep`). A key-safe
# partition key type change (`Enum8 -> Int8`) after that mutation has finished, but before the first load
# with the current version, makes the literal undecodable. The finished entry must not prevent the table
# from loading.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_05407"

${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS $TABLE;

    -- The mutation files are edited directly, so they have to be on a local disk.
    CREATE TABLE $TABLE (p Enum8('a' = 1, 'b' = 2), n Int64)
    ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
    SETTINGS storage_policy = 'default';

    INSERT INTO $TABLE VALUES ('a', 1), ('b', 2);

    ALTER TABLE $TABLE UPDATE n = n + 100 IN PARTITION 'a' WHERE 1 SETTINGS mutations_sync = 2;
    -- An older server version would have mutated the parts of all partitions; a global mutation does the same here.
    ALTER TABLE $TABLE UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2;

    ALTER TABLE $TABLE MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;
"

data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '$TABLE'")
first_mutation=$(${CLICKHOUSE_CLIENT} --query "
    SELECT mutation_id FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' ORDER BY mutation_id LIMIT 1")

${CLICKHOUSE_CLIENT} --query "DETACH TABLE $TABLE"

# Fabricate the legacy format: put the original literal back instead of the pinned partition id.
sed -i "s/IN PARTITION ID \\\\'1\\\\'/IN PARTITION \\\\'a\\\\'/" "${data_path}${first_mutation}"
grep -c "IN PARTITION \\\\'a\\\\'" "${data_path}${first_mutation}"

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE $TABLE"

${CLICKHOUSE_CLIENT} -m --query "
    SELECT command, is_done FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' ORDER BY mutation_id;
    SELECT p, n FROM $TABLE ORDER BY p;

    -- The table stays usable: new mutations of the converted partition key work.
    ALTER TABLE $TABLE UPDATE n = n + 1000 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 2;
    SELECT p, n FROM $TABLE ORDER BY p;

    DROP TABLE $TABLE;
"
