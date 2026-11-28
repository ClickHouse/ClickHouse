#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A volume of a storage policy that holds more than one disk is a JBOD volume. It limits the size of
# a part either with max_data_part_size_bytes or with max_data_part_size_ratio, never with both, and
# the ratio has to be not less than zero. With the ratio the limit is derived from the total space of
# the volume's disks, so a disk that reports no total space at all leaves the volume unlimited.
# Documented in docs/reference/engines/table-engines/mergetree-family/mergetree.mdx (storage policy
# configuration) and docs/reference/system-tables/storage_policies.mdx (volume_type,
# max_data_part_size). A JBOD volume is built from the configuration only, so the policies here are
# given to clickhouse-local in its own configuration file.

WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_jbod"
CONFIG="${WORK}/config.xml"
mkdir -p "${WORK}"

write_config()
{
    cat > "${CONFIG}" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
            <jbod_one>
                <type>local</type>
                <path>${WORK}/one/</path>
            </jbod_one>
            <jbod_two>
                <type>local</type>
                <path>${WORK}/two/</path>
            </jbod_two>
            <jbod_unsized>
                <type>object_storage</type>
                <object_storage_type>local</object_storage_type>
                <metadata_type>local</metadata_type>
                <path>${WORK}/unsized/</path>
            </jbod_unsized>
        </disks>
        <policies>
            <jbod_policy>
                <volumes>
                    <jbod_volume>$1</jbod_volume>
                </volumes>
            </jbod_policy>
        </policies>
    </storage_configuration>
</clickhouse>
XML
}

policy()
{
    ${CLICKHOUSE_LOCAL} --config-file="${CONFIG}" --path="${WORK}/db" -q "$1" 2>&1
}

# Two local disks and a ratio: the volume is a JBOD volume and its limit is derived from their space.

write_config "<disk>jbod_one</disk><disk>jbod_two</disk><max_data_part_size_ratio>2</max_data_part_size_ratio>"
policy "SELECT volume_type, max_data_part_size > 0 FROM system.storage_policies WHERE policy_name = 'jbod_policy'"

# The second disk is an object storage disk, which reports no total space. The ratio cannot be
# applied to it, so the volume keeps the unlimited default instead of a limit from a partial sum.

write_config "<disk>jbod_one</disk><disk>jbod_unsized</disk><max_data_part_size_ratio>0.5</max_data_part_size_ratio>"
policy "SELECT volume_type, max_data_part_size > 0 FROM system.storage_policies WHERE policy_name = 'jbod_policy'"

# A negative ratio is refused.

write_config "<disk>jbod_one</disk><disk>jbod_two</disk><max_data_part_size_ratio>-1</max_data_part_size_ratio>"
policy "SELECT count() FROM system.storage_policies" | grep -oF 'EXCESSIVE_ELEMENT_IN_CONFIG' | head -n 1

# Both forms of the limit at once are refused as well.

write_config "<disk>jbod_one</disk><disk>jbod_two</disk><max_data_part_size_bytes>134217728</max_data_part_size_bytes><max_data_part_size_ratio>0.5</max_data_part_size_ratio>"
policy "SELECT count() FROM system.storage_policies" | grep -oF 'EXCESSIVE_ELEMENT_IN_CONFIG' | head -n 1

rm -rf "${WORK:?}"
