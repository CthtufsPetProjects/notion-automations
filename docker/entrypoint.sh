#!/usr/bin/env bash

#set -o errexit
#set -o nounset
set +x

cmd="$*"

exec $cmd
