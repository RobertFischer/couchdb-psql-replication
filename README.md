# couchdb-psql-replication
Replicate your documents from CouchDB to PostgreSQL `jsonb`

# Overview

This repository produces an executable when you execute `stack build`. 
That executable will connect to a PSQL server, ensure the appropriate DDL
structures are in place, and then connect to the CouchDB server and sync
data over from the CouchDB server to the PSQL server. Once it is done running,
it will longpoll the CouchDB changelog for a period of time before resolving.
The intent is that the executable will be run under some auto-restarting 
service manager (like `daemontools`). Subsequent executions are 
non-destructive, so the goal is to restart regularly to ensure no changes
are missed (eg: the creation of a new database on CouchDB).

# Running locally

To run against a local Couch and local PSQL, using the DB and user off of the
`$USER` environment variable, just run `./run.sh`

# Configuration

Configuration is done through environment variables. They all have sane defaults,
which are printed to the log during start-up.

  * *COUCH_STARTING_SEQUENCE* -- How far back should the change polling go?
  * *COUCH_CONCURRENCY* -- How many concurrent Couch clients should we run?
  * *PSQL_CONCURRENCY* -- How many concurrent psql clients should we run?
  * *COUCH_POLL_SECONDS* -- Once syncing is done, how long should we keep polling for changes?
  * *PSQL_SCHEMA* -- What is the base name of the PSQL schemas?
  * *PSQL_USER* -- What is the user name to use to log into PSQL?
  * *PSQL_PASS* -- What is the password to use to log into PSQL?
  * *PSQL_DB* -- What PSQL database should we use?

