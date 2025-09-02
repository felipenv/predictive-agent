-- Procurement configuration for predictive maintenance parts inventory
-- This script creates the procurement schema and parts_stock table

-- Create the procurement schema
CREATE SCHEMA IF NOT EXISTS procurement;

-- Create the parts_stock table
CREATE TABLE IF NOT EXISTS procurement.parts_stock (
    id SERIAL PRIMARY KEY,
    part_number VARCHAR(100) NOT NULL,
    part_description TEXT,
    current_stock INTEGER NOT NULL DEFAULT 0,
    minimum_stock INTEGER NOT NULL DEFAULT 1,
    reorder_point INTEGER NOT NULL DEFAULT 5,
    unit_cost DECIMAL(10,2),
    supplier VARCHAR(100),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(part_number)
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_parts_stock_part_number ON procurement.parts_stock(part_number);
CREATE INDEX IF NOT EXISTS idx_parts_stock_current_stock ON procurement.parts_stock(current_stock);
CREATE INDEX IF NOT EXISTS idx_parts_stock_reorder_point ON procurement.parts_stock(reorder_point);

-- Add comments for documentation
COMMENT ON SCHEMA procurement IS 'Schema for procurement and inventory management';
COMMENT ON TABLE procurement.parts_stock IS 'Current stock levels and procurement information for maintenance parts';
COMMENT ON COLUMN procurement.parts_stock.part_number IS 'Unique part identifier (matches maintenance.service_parts.part)';
COMMENT ON COLUMN procurement.parts_stock.part_description IS 'Human-readable description of the part';
COMMENT ON COLUMN procurement.parts_stock.current_stock IS 'Current quantity in stock';
COMMENT ON COLUMN procurement.parts_stock.minimum_stock IS 'Minimum stock level before reordering';
COMMENT ON COLUMN procurement.parts_stock.reorder_point IS 'Stock level that triggers reorder';
COMMENT ON COLUMN procurement.parts_stock.unit_cost IS 'Cost per unit for procurement planning';
COMMENT ON COLUMN procurement.parts_stock.supplier IS 'Preferred supplier for this part';

-- Create a view to show stock status vs maintenance needs
CREATE OR REPLACE VIEW procurement.stock_vs_maintenance AS
SELECT 
    ps.part_number,
    ps.part_description,
    ps.current_stock,
    ps.minimum_stock,
    ps.reorder_point,
    ps.unit_cost,
    ps.supplier,
    COALESCE(SUM(sp.quantity), 0) as total_maintenance_need,
    ps.current_stock - COALESCE(SUM(sp.quantity), 0) as stock_after_maintenance,
    CASE 
        WHEN ps.current_stock < ps.reorder_point THEN 'REORDER'
        WHEN ps.current_stock < ps.minimum_stock THEN 'LOW_STOCK'
        WHEN ps.current_stock < COALESCE(SUM(sp.quantity), 0) THEN 'INSUFFICIENT_FOR_MAINTENANCE'
        ELSE 'SUFFICIENT'
    END as stock_status
FROM procurement.parts_stock ps
LEFT JOIN maintenance.service_parts sp ON ps.part_number = sp.part
GROUP BY ps.id, ps.part_number, ps.part_description, ps.current_stock, 
         ps.minimum_stock, ps.reorder_point, ps.unit_cost, ps.supplier;

-- Grant permissions (adjust as needed for your setup)
-- GRANT USAGE ON SCHEMA procurement TO optimus;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA procurement TO optimus;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA procurement TO optimus;
