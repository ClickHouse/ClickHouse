#!/bin/bash
set -e

# If --no-checkout is used in the main script, git bisect sets BISECT_HEAD
# instead of checking out the commit.
if [[ "$NO_CHECKOUT" == "true" ]]; then
  COMMIT_REF="BISECT_HEAD"
else
  COMMIT_REF="HEAD"
fi

COMMIT_SHA=$(git rev-parse $COMMIT_REF)
COMMIT_SHORT_SHA=$(git rev-parse --short $COMMIT_REF)
COMMIT_AUTHOR=$(git show -s --format='%an' $COMMIT_REF)
COMMIT_AUTHOR_EMAIL=$(git show -s --format='%ae' $COMMIT_REF)
COMMIT_DATE=$(git show -s --format='%ad' $COMMIT_REF)
COMMIT_SUBJECT=$(git show -s --format='%s' $COMMIT_REF)
COMMIT_MESSAGE=$(git log -1 --pretty=%B $COMMIT_REF)

export COMMIT_SHA COMMIT_SHORT_SHA COMMIT_AUTHOR COMMIT_AUTHOR_EMAIL COMMIT_DATE COMMIT_SUBJECT COMMIT_MESSAGE

# Check if COMMIT_SUBJECT contains a version pattern like "release/xx.xx"
if [[ "$COMMIT_SUBJECT" =~ release/[0-9]{2}\.[0-9]{1,2} ]]; then
  # Extract the matched release version (e.g., "release/24.8")
  BUILD_VERSION=$(echo "$COMMIT_SUBJECT" | grep -o "release/[0-9]\{2\}\.[0-9]\{1,2\}")
else
  BUILD_VERSION=''
fi

export BUILD_VERSION
export CH_PATH="$SCRIPT_DIR/data/clickhouse"
mkdir -p "$SCRIPT_DIR/data"

# 255 - exit from bisect
# 125 - skip commit
# Try to download. If a binary not found -- compile it.
$SCRIPT_DIR/helpers/download.sh "$COMMIT_SHA" || exit 125
# Prepare the env.
$SCRIPT_DIR/env/${ENV_OPTION}.sh $GIT_WORK_TREE $CH_PATH || exit 255
# Actual testing.
set +e
(
  cd $GIT_WORK_TREE || exit 1
  $TEST_COMMAND $GIT_WORK_TREE
)
RESULT=$?

if [[ "$NO_CHECKOUT" != "true" ]]; then
  git -C "$GIT_WORK_TREE" reset --hard > /dev/null 2>&1
fi

# If the test passes (no bug), return 0; otherwise, return 1
if [ $RESULT -eq 0 ]; then
  echo "Commit $COMMIT_SHA is good"
  exit 0  # Good commit
else
  echo "Commit $COMMIT_SHA is bad"
  exit 1  # Bad commit
fi
