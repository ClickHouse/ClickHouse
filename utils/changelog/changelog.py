#!/usr/bin/env python3
# In our CI this script runs in style-test containers

# The main script is moved to tests/ci/changelog.py
# It depends on the ci scripts too hard to keep it here
# Here's only a wrapper around it for the people who used to it

import subprocess
import sys
from pathlib import Path

SCRIPT_PATH = (Path(__file__).parents[2] / "tests/ci/changelog.py").absolute()

if __name__ == "__main__":
    subprocess.check_call(["python3", SCRIPT_PATH, *sys.argv[1:]])
