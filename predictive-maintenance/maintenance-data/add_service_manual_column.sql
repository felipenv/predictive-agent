-- Migration script to add service_manual column to existing maintenance.service_manual table
-- Run this script to update the existing database schema

-- Add the service_manual column
ALTER TABLE maintenance.service_manual 
ADD COLUMN IF NOT EXISTS service_manual TEXT;

-- Add comment for the new column
COMMENT ON COLUMN maintenance.service_manual.service_manual IS 'Complete service manual content in markdown format';

-- Update the table comment
COMMENT ON TABLE maintenance.service_manual IS 'Service manual information for equipment maintenance';
