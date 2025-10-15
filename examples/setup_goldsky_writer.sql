-- Setup script for goldsky_writer role
-- Safe to run multiple times; it will only create the role if it doesn't exist
-- No secrets stored here. Set the password separately using \password in psql.

-- Allow passing database name as a psql variable
-- Usage example:
--   psql -v DBNAME="postgres" -f examples/setup_goldsky_writer.sql

\set DBNAME 'postgres'

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'goldsky_writer'
  ) THEN
    CREATE ROLE goldsky_writer LOGIN;
  END IF;
END
$$;

-- Grant database connect (adjust DBNAME as needed)
GRANT CONNECT ON DATABASE :DBNAME TO goldsky_writer;

-- Schema usage
GRANT USAGE ON SCHEMA public TO goldsky_writer;
-- Also allow creating new tables in the public schema so sinks can auto-create target tables
GRANT CREATE ON SCHEMA public TO goldsky_writer;

-- Table privileges on existing tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO goldsky_writer;

-- Sequence privileges (for SERIAL/BIGSERIAL or identity columns)
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO goldsky_writer;

-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO goldsky_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO goldsky_writer;

-- Reminder: set a secure password interactively after running this script
-- In psql: \password goldsky_writer