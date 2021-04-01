#!/bin/sh

set -eu

exec /esbridge -migrate "$ESBRIDGE_INDEX" -batch-size "$ESBRIDGE_BATCH_SIZE"
