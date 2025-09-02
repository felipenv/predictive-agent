-- Migration script to update the maintenance schema
-- This will drop old tables and recreate them with the new structure

-- Drop existing tables if they exist (this will remove all data)
DROP TABLE IF EXISTS maintenance.service_parts CASCADE;
DROP TABLE IF EXISTS maintenance.service_manual CASCADE;

-- Drop indexes if they exist
DROP INDEX IF EXISTS idx_service_manual_unique_id;
DROP INDEX IF EXISTS idx_service_manual_part_numbers;
DROP INDEX IF EXISTS idx_service_parts_equipment_id;
DROP INDEX IF EXISTS idx_service_parts_part;

-- Recreate the service_manual table with new structure
CREATE TABLE maintenance.service_manual (
    id SERIAL PRIMARY KEY,
    unique_id INTEGER NOT NULL,
    service_description TEXT NOT NULL,
    UNIQUE(unique_id)
);

-- Recreate the service_parts table
CREATE TABLE maintenance.service_parts (
    id SERIAL PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    part VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (equipment_id) REFERENCES maintenance.service_manual(unique_id) ON DELETE CASCADE
);

-- Recreate indexes
CREATE INDEX idx_service_manual_unique_id ON maintenance.service_manual(unique_id);
CREATE INDEX idx_service_parts_equipment_id ON maintenance.service_parts(equipment_id);
CREATE INDEX idx_service_parts_part ON maintenance.service_parts(part);

-- Add comments for documentation
COMMENT ON SCHEMA maintenance IS 'Schema for maintenance-related data including service manuals and parts';
COMMENT ON TABLE maintenance.service_manual IS 'Service manual information for equipment maintenance';
COMMENT ON COLUMN maintenance.service_manual.unique_id IS 'Unique identifier for the equipment (matches the MLRun dataset unique_id)';
COMMENT ON COLUMN maintenance.service_manual.service_description IS 'Description of the maintenance service required';

COMMENT ON TABLE maintenance.service_parts IS 'Detailed parts list with quantities for each equipment maintenance service';
COMMENT ON COLUMN maintenance.service_parts.equipment_id IS 'Foreign key to service_manual.unique_id';
COMMENT ON COLUMN maintenance.service_parts.part IS 'Part number or description';
COMMENT ON COLUMN maintenance.service_parts.quantity IS 'Quantity of parts required for the service';

-- Grant permissions (adjust as needed for your setup)
-- GRANT USAGE ON SCHEMA maintenance TO optimus;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA maintenance TO optimus;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA maintenance TO optimus;
