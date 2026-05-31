#!/usr/bin/env sh
set -eu

node --test strait-server/src/web/assets/*.test.mjs
