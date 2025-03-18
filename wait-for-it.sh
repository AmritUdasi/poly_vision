#!/bin/bash
# wait-for-it.sh script content here
# You can download it from: https://github.com/vishnubob/wait-for-it

# Usage example in your case:
# ./wait-for-it.sh postgres:5432 -- python -m poly_vision

host="$1"
user="$2"
port="$3"
db="$4"

shift
shift
cmd="$@"

until PGPASSWORD=$TRADE_DB_PASSWORD psql -p "$port" -h "$host" -U "$user" -d "$db" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Postgres is up - executing command"
