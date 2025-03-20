#!/bin/bash
# wait-for-it.sh script content here
# You can download it from: https://github.com/vishnubob/wait-for-it

# Usage example in your case:
# ./wait-for-it.sh postgres:5432 -- python -m poly_vision

# Using the environment variable names from your .env file
host="${1:-postgres}"  # POSTGRES_SEEDS from .env
user="${2:-$POSTGRES_USER}"
port="${3:-$DB_PORT}"  # DB_PORT from .env
db="${4:-$POSTGRES_DB}"
pass="${5:-$POSTGRES_PASSWORD}"

shift 5 || true

until PGPASSWORD=$pass psql -h "$host" -U "$user" -p "$port" -d "$db" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec "$@"
