#!/bin/bash

export PSQL_USER="$USER"
export PSQL_DB="$PSQL_USER"  # assumes you're in a DB matching your username
read -s -p "PSQL password for $USER:" PSQL_PASS
export PSQL_PASS

# Comment these out for ludicrious speed
# Uncomment these for debugging 
#export COUCH_CONCURRENCY=1
#export PSQL_CONCURRENCY=2

echo

stack build 2>&1 && nice --adjustment=19 stack exec exe 
