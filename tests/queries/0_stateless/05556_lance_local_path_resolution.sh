#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A local dataset path is resolved relative to `user_files_path`, and `Lance` opens exactly the path that was
# checked: a `file://` or `s3://` URI is not reinterpreted by `Lance` to escape `user_files_path`.
outside_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside"
inside_dir="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${outside_dir:?}" "${inside_dir:?}"
trap 'rm -rf "${outside_dir:?}" "${inside_dir:?}"' EXIT

mkdir -p "${outside_dir}" "${inside_dir}"
outside_dir=$(cd "${outside_dir}" && pwd)
cp -R "${CUR_DIR}/data_lance/basic.lance" "${outside_dir}/"
cp -R "${CUR_DIR}/data_lance/basic.lance" "${inside_dir}/"

for path in "file://${outside_dir}/basic.lance" "s3://bucket/basic.lance"
do
    ${CLICKHOUSE_CLIENT} --enable_lance=1 --query "DESCRIBE lanceLocal('${path}')" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
    ${CLICKHOUSE_CLIENT} --enable_lance=1 --query "SELECT count() FROM lanceLocal('${path}')" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
done

# A relative path is relative to `user_files_path`.
relative_dir=$(basename "${inside_dir}")
${CLICKHOUSE_CLIENT} --enable_lance=1 --query "SELECT count() FROM lanceLocal('${relative_dir}/basic.lance')"
${CLICKHOUSE_CLIENT} --enable_lance=1 --query "SELECT count() FROM lanceLocal('${inside_dir}/basic.lance')"
