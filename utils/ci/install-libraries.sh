#!/usr/bin/env bash
set -e -x

source default-config

./install-os-packages.sh libicu-dev
./install-os-packages.sh libreadline-dev
