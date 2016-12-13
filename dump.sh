#!/bin/bash

pg_dump --verbose --no-owner --schema-only --quote-all-identifiers "$USER" > schema.sql && less schema.sql
