#!/bin/bash
# Import schema to Neon PostgreSQL

set -e

echo "📤 Importing schema to Neon..."

# Load Neon connection string
if [ ! -f .env.neon ]; then
    echo "❌ Error: .env.neon not found"
    echo "   Please create it with NEON_DATABASE_URL=..."
    exit 1
fi

source .env.neon

if [ -z "$NEON_DATABASE_URL" ]; then
    echo "❌ Error: NEON_DATABASE_URL not set in .env.neon"
    exit 1
fi

if [ ! -f migration/schema.sql ]; then
    echo "❌ Error: migration/schema.sql not found"
    echo "   Run scripts/export_supabase_schema.sh first"
    exit 1
fi

# Import schema
psql "$NEON_DATABASE_URL" < migration/schema.sql

echo "✅ Schema imported to Neon"
echo ""
echo "Verifying tables..."
psql "$NEON_DATABASE_URL" -c "\dt"

echo ""
echo "Next: Run python3 scripts/migrate_to_neon.py to migrate data"
