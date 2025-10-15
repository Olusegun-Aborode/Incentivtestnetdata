#!/bin/bash

# Goldsky CLI Database Integration Example
# This script demonstrates how to use Goldsky CLI with a PostgreSQL database

# Store connection string securely in a variable using single quotes
DB_URL='postgresql://postgres:N5zhNHZ?HWGQ*.n@db.dbravpbyfdfvjfooenvf.supabase.co:5432/postgres'

# Create a .env file for secure credential storage (recommended approach)
echo "Creating .env file for secure credential storage..."
cat > .env << EOL
# Database credentials - DO NOT commit this file to version control
DB_URL=${DB_URL}
EOL
echo "Created .env file with database credentials"

# Make the script executable
chmod +x examples/db-connect.sh

echo -e "\n=== Goldsky CLI Database Examples ==="

# Example 1: Create a dataset using the database connection
echo -e "\n1. Creating a dataset with database connection:"
echo "goldsky dataset create my-dataset --db-url '${DB_URL}'"

# Example 2: Using environment variables with Goldsky
echo -e "\n2. Using environment variables (recommended):"
echo "export DB_URL='${DB_URL}'"
echo "goldsky dataset create my-dataset --db-url \"\$DB_URL\""

# Example 3: Using a configuration file
echo -e "\n3. Using a configuration file:"
cat > goldsky.config.json << EOL
{
  "database": {
    "url": "${DB_URL}"
  },
  "project": {
    "name": "my-goldsky-project"
  }
}
EOL
echo "Created goldsky.config.json"
echo "goldsky project init --config goldsky.config.json"

# Example 4: Using Goldsky secrets management (most secure)
echo -e "\n4. Using Goldsky secrets management (most secure):"
echo "goldsky secret set DATABASE_URL '${DB_URL}'"
echo "goldsky dataset create my-dataset --use-secret DATABASE_URL"

echo -e "\n=== Security Best Practices ==="
echo "1. Never commit credentials to version control"
echo "2. Use environment variables or secret management"
echo "3. Restrict database user permissions"
echo "4. Rotate credentials regularly"

echo -e "\nTo run any of these commands, remove the echo statements or copy them to your terminal."