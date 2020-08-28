#!/bin/sh

# sudo apt install python3-pip
# sudo pip3 install gitpython termcolor

# Go to GitHub.
# In top-right corner, click to your profile icon.
# Choose "Settings".
# Choose "Developer settings".
# Choose "Personal access tokens".
# Choose "Generate new token".

# Don't check any checkboxes.

# Run as:
# ./list_backports.sh --token your-token


set -e
SCRIPTPATH=$(readlink -f "$0")
SCRIPTDIR=$(dirname "$SCRIPTPATH")
PYTHONPATH="$SCRIPTDIR" python3 -m github "$@"
