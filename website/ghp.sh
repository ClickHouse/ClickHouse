#!/usr/bin/env bash
set -ex
BASEDIR=$(mktemp)
REMOTE_URL=$(git remote -v | grep origin | grep push | awk '{print $2;}')
rm -rf "${BASEDIR}"
git clone "${REMOTE_URL}" "${BASEDIR}"
(cd "${BASEDIR}"; git checkout gh-pages; git rm -r *)
cat publish.txt | while read FILENAME
do
    cp "${FILENAME}" "${BASEDIR}/"
    (cd "${BASEDIR}"; git add "${FILENAME}")
done
(cd "${BASEDIR}"; git commit -a -m "Website update"; git push origin gh-pages)
rm -rf "${BASEDIR}"
