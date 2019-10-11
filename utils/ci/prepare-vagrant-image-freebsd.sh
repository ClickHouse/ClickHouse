#!/usr/bin/env bash
set -e -x

source default-config

./install-os-packages.sh vagrant-virtualbox

pushd "vagrant-freebsd"
vagrant up
vagrant ssh-config > vagrant-ssh
ssh -F vagrant-ssh default 'uname -a'
popd
