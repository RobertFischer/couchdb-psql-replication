#!/bin/bash

export PSQL_USER="$USER"
export PSQL_PASS=`read -s -p "PSQL password for $USER:"`

echo

exec stack exec exe
