#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dataset directory inside `user_files_path` must not expose a dataset outside of it through
# symbolic links: `Lance` reads the files itself and follows the links.
outside_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside"
inside_dir="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${outside_dir:?}" "${inside_dir:?}"
trap 'rm -rf "${outside_dir:?}" "${inside_dir:?}"' EXIT

mkdir -p "${outside_dir}" "${inside_dir}/escaping.lance"
cp -R "${CUR_DIR}/data_lance/basic.lance" "${outside_dir}/"
ln -s "${outside_dir}/basic.lance/_versions" "${inside_dir}/escaping.lance/_versions"
ln -s "${outside_dir}/basic.lance/_transactions" "${inside_dir}/escaping.lance/_transactions"
ln -s "${outside_dir}/basic.lance/data" "${inside_dir}/escaping.lance/data"

# A symbolic link inside of the dataset directory is allowed.
cp -R "${CUR_DIR}/data_lance/basic.lance" "${inside_dir}/contained.lance"
mv "${inside_dir}/contained.lance/data" "${inside_dir}/contained.lance/real_data"
ln -s "${inside_dir}/contained.lance/real_data" "${inside_dir}/contained.lance/data"

${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --query "DESCRIBE lanceLocal('${inside_dir}/escaping.lance')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --query "SELECT count() FROM lanceLocal('${inside_dir}/escaping.lance')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --query "SELECT count() FROM lanceLocal('${inside_dir}/contained.lance')"
