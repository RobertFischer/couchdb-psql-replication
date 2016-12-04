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

# Configuration

Configuration is done through environment variables.
