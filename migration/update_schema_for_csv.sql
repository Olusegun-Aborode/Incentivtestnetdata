-- Update Neon schema to match CSV structure
-- Add missing columns to blocks and transactions tables

-- Blocks table - add missing columns
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS block_number BIGINT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS nonce TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS sha3_uncles TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS logs_bloom TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS transactions_root TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS state_root TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS receipts_root TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS miner TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS difficulty TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS total_difficulty TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS size BIGINT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS extra_data TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS transaction_count INTEGER;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS chain TEXT;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMPTZ;

-- Transactions table - add missing columns  
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS nonce TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS transaction_index INTEGER;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS gas BIGINT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS input TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS block_hash TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS chain TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMPTZ;

-- Rename number to block_number in blocks for consistency
UPDATE blocks SET block_number = number WHERE block_number IS NULL;
