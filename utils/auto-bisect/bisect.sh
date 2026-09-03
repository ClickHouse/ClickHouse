#!/bin/bash
set -e
# Aka GIGACHAD BISECTOR
# Example:
# ./bisect.sh --good 4c191319 --bad 09a09ca5 -p /home/nik/work/ClickHouse/ [--walker]

export START_COMMIT=""
export END_COMMIT=""
export TEST_COMMAND=""
export GIT_WORK_TREE=${CH_ROOT:-}
export BUILD_DIR=${CH_ROOT:+$CH_ROOT/build}
export ENV_OPTION="single"
export USE_WALKER=false
export NO_CHECKOUT=true
export COMMITS_ARG=""
export SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PRIVATE=false # Default to public CI builds

# clickhouse-test will use its client
export CLICKHOUSE_BINARY="$SCRIPT_DIR/data/clickhouse"

# Usage function to display help
usage() {
  echo "Usage: $0 --good <start_commit> --bad <end_commit> --path <path> --test <test_command> [--env <env_option>] [--walker] [--private]"
  echo "  --good       Starting commit hash for bisect"
  echo "  --bad        Ending commit hash for bisect"
  echo "  --path       Path to ClickHouse repository. CH_ROOT env variable will be used by default"
  echo "  --build-missing-binary  (Optional) Compile missing binaries if binary from CI is missing"
  echo "  --build-directory       Path to the build directory in case if we will decide to compile the binary"
  echo "  --test       Path to test script/command to run for each commit (default: test.sh)"
  echo "  --env        Environment option: single (default), replicateddb, sharedcatalog"
  echo "  --walker     Run test on each first-parent commit (instead of bisect)"
  echo "  --walker-steps  Used with --walker: run test on only N evenly spaced commits between good and bad"
  echo "  --walker-commits  Comma/space-separated list of commit hashes to use in walker mode (overrides --good/--bad)"
  echo "  --private    (Optional) Use private CI builds (requires credentials; see README)."
  exit 1
}

SHORT=g:b:p:d:t:w:c:
LONG=good:,bad:,path:,build-directory:,test:,env:,walker,walker-steps:,walker-commits:,build-missing-binary,private

PARSED=$(getopt --options=$SHORT --longoptions=$LONG --name "$0" -- "$@")
if [[ $? -ne 0 ]]; then
    usage
fi

# Apply the parsed arguments
eval set -- "$PARSED"

# Process options
while true; do
  case "$1" in
    -g|--good)
      START_COMMIT="$2"
      shift 2
      ;;
    -b|--bad)
      END_COMMIT="$2"
      shift 2
      ;;
    -p|--path)
      GIT_WORK_TREE="$2"
      shift 2
      ;;
    --build-missing-binary)
      export COMPILE=true
      shift
      ;;
    -d|--build-directory)
      BUILD_DIR="$2"
      shift 2
      ;;
    -t|--test)
      TEST_COMMAND="$2"
      shift 2
      ;;
    --env)
      ENV_OPTION="$2"
      if [[ "$ENV_OPTION" != "single" && "$ENV_OPTION" != "replicateddb" && "$ENV_OPTION" != "sharedcatalog" && "$ENV_OPTION" != "nothing" ]]; then
        echo "Invalid value for --env: $ENV_OPTION"
        echo "Allowed values are: single, replicateddb, sharedcatalog, nothing"
        exit 1
      fi
      shift 2
      ;;
    --walker)
      USE_WALKER=true
      shift
      ;;
    --walker-commits)
      COMMITS_ARG="$2"
      shift 2
      ;;
    -w|--walker-steps)
      WALKER_STEPS="$2"
      if ! [[ "$WALKER_STEPS" =~ ^[0-9]*$ && "$WALKER_STEPS" -gt 0 ]]; then
        echo "Invalid value for --walker-steps: $WALKER_STEPS"
        echo "It must be a positive integer."
        exit 1
      fi
      shift 2
      ;;
    --private)
      PRIVATE=true
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "$TEST_COMMAND" ]]; then
  TEST_COMMAND="$SCRIPT_DIR/tests/test.sh"
  echo "INFO: --test was not specified, using default: $TEST_COMMAND"
fi

# Resolve relative paths against SCRIPT_DIR
if [[ "$TEST_COMMAND" != /* ]]; then
  TEST_COMMAND="$SCRIPT_DIR/$TEST_COMMAND"
fi

# Check if all required arguments are provided
if [[ -z "$GIT_WORK_TREE" ]]; then
  echo "Error: Missing required argument: --path"
  usage
fi

if $USE_WALKER; then
  # For walker mode: either --good/--bad, or --walker-commits must be set
  if [[ ( -z "$START_COMMIT" || -z "$END_COMMIT" ) && -z "$COMMITS_ARG" ]]; then
    echo "Error: In walker mode, specify --good and --bad, or --walker-commits."
    usage
  fi
  if [[ -n "$COMMITS_ARG" && ( -n "$START_COMMIT" || -n "$END_COMMIT" ) ]]; then
    echo "Warning: --walker-commits provided; ignoring --good/--bad."
  fi
else
  # In bisect mode, --good and --bad are required, --walker-commits not allowed
  if [[ -z "$START_COMMIT" || -z "$END_COMMIT" ]]; then
    echo "Error: --good and --bad are required in bisect mode."
    usage
  fi
  if [[ -n "$COMMITS_ARG" ]]; then
    echo "Error: --walker-commits is only allowed in walker mode."
    usage
  fi
fi

# Confirm parsed arguments
echo "Start commit: $START_COMMIT"
echo "End commit: $END_COMMIT"
echo "Work tree path: $GIT_WORK_TREE"
echo "Test command: $TEST_COMMAND"
echo "Environment option: $ENV_OPTION"
echo "Walker mode: $USE_WALKER"
echo "Private mode: $PRIVATE"
if [[ -n "$WALKER_STEPS" ]]; then
  echo "Walker steps: $WALKER_STEPS"
fi

# This env variable is used by git as default --work-tree
export GIT_WORK_TREE
export BUILD_DIR
export GIT_DIR="$GIT_WORK_TREE/.git"
export NO_CHECKOUT
export PRIVATE

check_path_exists() {
  if [ -d "$GIT_WORK_TREE" ]; then
    echo "Path exists: $GIT_WORK_TREE"
  else
    echo "Error: Path does not exist: $GIT_WORK_TREE"
    exit 1
  fi
}

# Function to check if commit exists
check_commit_exists() {
  local commit="$1"
  if git -C "$GIT_WORK_TREE" cat-file -e "$commit" 2>/dev/null; then
    echo "Commit exists: $commit"
  else
    echo "Error: Commit does not exist: $commit"
    exit 1
  fi
}

# Function to check if script exists and is executable
check_script_executable() {
  if [ -x "$TEST_COMMAND" ]; then
    echo "Script exists and is executable: $TEST_COMMAND"
  else
    echo "Error: Script does not exist or is not executable: $TEST_COMMAND"
    exit 1
  fi
}

# Call the checks before running main logic
if [[ "$PRIVATE" == "true" ]]; then
  if [[ -z "${CH_CI_USER:-}" || -z "${CH_CI_PASSWORD:-}" ]]; then
    echo "Error: --private requires CH_CI_USER and CH_CI_PASSWORD to be set."
    exit 1
  fi
fi

check_path_exists
if [[ -z "$COMMITS_ARG" ]]; then
  check_commit_exists "$START_COMMIT"
  check_commit_exists "$END_COMMIT"
fi
check_script_executable

echo

cleanup() {
  echo "Cleaning up..."
  # Kill all local ClickHouse server processes
  (ps aux | grep -E '[c]lickhouse[- ]server' | awk '{print $2}' | xargs kill -9) 2>/dev/null || true
  # Kill MinIO (identified by port 11111)
  (lsof -ti :11111 | xargs kill -9) 2>/dev/null || true
  # Git bisect reset (only matters for bisect mode)
  git -C "$GIT_WORK_TREE" bisect reset > /dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p $SCRIPT_DIR/data

if $USE_WALKER; then
  echo "Running in WALKER mode (first-parent linear history)..."
  rm -f "$SCRIPT_DIR/data/walker.log"
  if [[ -n "$COMMITS_ARG" ]]; then
    echo "Walker: Using user-specified commits list"
    # Split comma or space separated input to array
    IFS=', ' read -r -a COMMITS <<< "$COMMITS_ARG"
    # Check all commits exist
    for commit in "${COMMITS[@]}"; do
      check_commit_exists "$commit"
    done
  else
    COMMITS=(
      $(git -C "$GIT_WORK_TREE" rev-list --first-parent --reverse "$START_COMMIT^..$END_COMMIT" | while read commit; do
          MSG=$(git -C "$GIT_WORK_TREE" log --format=%B -n 1 "$commit")
          if [[ "$MSG" != *"Automatic synchronization with upstream"* ]]; then
              echo "$commit"
          fi
      done)
    )
  fi
  TOTAL_COMMITS=${#COMMITS[@]}
  # If walker-steps specified, filter commits
  if [[ -n "$WALKER_STEPS" && "$WALKER_STEPS" -lt "$TOTAL_COMMITS" ]]; then
    echo "Selecting $WALKER_STEPS evenly spaced commits out of $TOTAL_COMMITS"
    SELECTED_COMMITS=()
    for ((i=0; i<$WALKER_STEPS; i++)); do
      if [[ "$WALKER_STEPS" -eq 1 ]]; then
        idx=0
      else
        idx=$(( i * (TOTAL_COMMITS - 1) / (WALKER_STEPS - 1) ))
      fi
      SELECTED_COMMITS+=("${COMMITS[$idx]}")
    done
    # Remove duplicates in case of low step counts (can happen if total commits is small)
    COMMITS=($(printf "%s\n" "${SELECTED_COMMITS[@]}" | awk '!seen[$0]++'))
    TOTAL_COMMITS=${#COMMITS[@]}
  fi

  INDEX=1
  for commit in "${COMMITS[@]}"; do
    echo "[$INDEX/$TOTAL_COMMITS] Testing commit: $commit" | tee -a "$SCRIPT_DIR/data/walker.log"
    ((INDEX++))
    # Write commit SHA to BISECT_HEAD so bisect_step.sh can read it without
    # checking out the commit (working tree stays at its current state).
    echo "$commit" > "$GIT_WORK_TREE/.git/BISECT_HEAD"

    set +e
    "$SCRIPT_DIR/bisect_step.sh" | tee -a "$SCRIPT_DIR/data/walker.log"
    EXIT_CODE=${PIPESTATUS[0]}
    echo "EXIT_CODE=$EXIT_CODE" | tee -a "$SCRIPT_DIR/data/walker.log"
    set -e

  done

  rm -f "$GIT_WORK_TREE/.git/BISECT_HEAD"

else
  echo "Running in BISECT mode..."

  if [ "$NO_CHECKOUT" = true ]; then
    git -C "$GIT_WORK_TREE" bisect start --first-parent --no-checkout
  else
    git -C "$GIT_WORK_TREE" bisect start --first-parent
  fi

  git -C "$GIT_WORK_TREE" bisect bad "$END_COMMIT"
  git -C "$GIT_WORK_TREE" bisect good "$START_COMMIT"

  git -C "$GIT_WORK_TREE" bisect run "$SCRIPT_DIR/bisect_step.sh"

  echo
  git -C "$GIT_WORK_TREE" bisect log > "$SCRIPT_DIR/data/bisect_log.txt"
fi
