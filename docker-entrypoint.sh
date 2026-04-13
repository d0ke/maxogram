#!/usr/bin/env sh
set -eu

cd /app

python -m maxogram check-config
python -m maxogram db-upgrade

exec "$@"
