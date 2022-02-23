#!/bin/sh
set -e

pg_dump --compress=0 --format=plain --file=$BK_DATABASE/$BK_TABLE.sql --table=$BK_TABLE $BK_HOST/$BK_DATABASE