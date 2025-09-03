-- Database configuration for predictive maintenance service manual data
-- This script creates the maintenance schema and service_manual table in the optimus database

-- Create the maintenance schema
CREATE SCHEMA IF NOT EXISTS maintenance;

-- Create the service_manual table
CREATE TABLE IF NOT EXISTS maintenance.service_manual (
    id SERIAL PRIMARY KEY,
    unique_id INTEGER NOT NULL,
    service_description TEXT NOT NULL,
    UNIQUE(unique_id)
);

-- Create the service_parts table
CREATE TABLE IF NOT EXISTS maintenance.service_parts (
    id SERIAL PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    part VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (equipment_id) REFERENCES maintenance.service_manual(unique_id) ON DELETE CASCADE
);

-- Create index on unique_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_service_manual_unique_id ON maintenance.service_manual(unique_id);

-- Create index on equipment_id for parts lookups
CREATE INDEX IF NOT EXISTS idx_service_parts_equipment_id ON maintenance.service_parts(equipment_id);

-- Create index on part for searching specific parts
CREATE INDEX IF NOT EXISTS idx_service_parts_part ON maintenance.service_parts(part);

-- Add comments for documentation
COMMENT ON SCHEMA maintenance IS 'Schema for maintenance-related data including service manuals and parts';
COMMENT ON TABLE maintenance.service_manual IS 'Service manual information for equipment maintenance including part numbers';
COMMENT ON COLUMN maintenance.service_manual.unique_id IS 'Unique identifier for the equipment (matches the MLRun dataset unique_id)';
COMMENT ON COLUMN maintenance.service_manual.service_description IS 'Description of the maintenance service required';
COMMENT ON COLUMN maintenance.service_manual.part_numbers IS 'Array of part numbers needed for this maintenance service';


-- Grant permissions (adjust as needed for your setup)
-- GRANT USAGE ON SCHEMA maintenance TO your_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA maintenance TO your_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA maintenance TO your_user;
