#!/usr/bin/env bash
set -e -x

[[ -r "vagrant-${1}/vagrant-ssh" ]] || die "Run prepare-vagrant-image-... first."

pushd vagrant-$1

shopt -s extglob

vagrant ssh -c "mkdir -p ClickHouse"
scp -q -F vagrant-ssh -r ../../!(*build*) default:~/ClickHouse
vagrant ssh -c "cd ClickHouse/ci; $2"

popd
