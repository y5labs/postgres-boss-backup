#!/bin/sh
set -e

pg_dumpall --compress=0 --format=plain --file=test.sql