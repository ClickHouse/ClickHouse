#!/usr/bin/env bash
set -ex
BASEDIR=$(dirname $(readlink -f $0))
TMPDIR=$(mktemp)
REMOTE_URL=$(git remote -v | grep origin | grep push | awk '{print $2;}')
rm -rf "${TMPDIR}"
git clone "${REMOTE_URL}" "${TMPDIR}"
(cd "${TMPDIR}"; git checkout gh-pages; git rm -r *)
ls -1 "${BASEDIR}/public/" | while read FILENAME
do
    cp "${BASEDIR}/public/${FILENAME}" "${TMPDIR}/"
    (cd "${TMPDIR}"; git add "${FILENAME}")
done
(cd "${TMPDIR}"; git commit -a -m "Website update"; git push origin gh-pages)
rm -rf "${TMPDIR}"
