#!/usr/bin/env sh
set -eu

node --test janus-server/src/web/assets/*.test.mjs
