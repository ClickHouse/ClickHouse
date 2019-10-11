#!/usr/bin/env bash
set -e -x

source default-config

command -v docker > /dev/null || die "You need to install Docker"
docker ps > /dev/null || die "You need to have access to Docker: run '$SUDO usermod -aG docker $USER' and relogin"
