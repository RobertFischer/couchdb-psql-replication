#!/bin/bash

pg_dump --schema-only --quote-all-identifiers "$USER" > schema.sql && less schema.sql
