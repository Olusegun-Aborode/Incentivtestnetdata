#!/bin/bash
# Usage helper for connecting to Supabase Postgres with psql
# - If you pass a full connection URL as the first argument, the script will run psql with it
# - Otherwise, it prints safe examples and guidance
#
# Note: Avoid committing real credentials. Prefer environment variables or a secrets manager.

set -euo pipefail

if [ "${1-}" != "" ]; then
  CONN_URL="$1"
  echo "Connecting with psql using provided URL..."
  echo "Tip: ensure you used single quotes around the URL in your shell to avoid special character expansion"
  echo "Running: psql \"$CONN_URL\""
  psql "$CONN_URL"
  exit $?
fi

cat <<'EOF'

=== psql connection examples (Supabase) ===

1) Single-quoted full URL with SSL required (recommended)
   Replace YOUR_PROJECT_REF and REPLACE_PASSWORD, then run:

   psql 'postgresql://postgres:REPLACE_PASSWORD@db.YOUR_PROJECT_REF.supabase.co:5432/postgres?sslmode=require'

2) Using environment variables (more secure, avoids quoting issues)
   export PGUSER=postgres
   export PGPASSWORD='REPLACE_PASSWORD'
   export PGHOST='db.YOUR_PROJECT_REF.supabase.co'
   export PGPORT=5432
   export PGDATABASE=postgres
   export PGSSLMODE=require
   psql

3) Quick DNS sanity check (helps diagnose hostname errors)
   nslookup supabase.co
   nslookup db.YOUR_PROJECT_REF.supabase.co

Notes:
- Always quote the entire connection URL with single quotes if it contains special characters (e.g. * ! ? &)
- Ensure your PATH includes Homebrew libpq binaries if needed: export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
- If you just updated your shell config, run: source ~/.zshrc
- Copy the exact connection string/host from your Supabase dashboard to avoid typos

Usage:
  ./examples/db-connect.sh 'postgresql://postgres:REPLACE_PASSWORD@db.YOUR_PROJECT_REF.supabase.co:5432/postgres?sslmode=require'
  ./examples/db-connect.sh    # prints examples

EOF