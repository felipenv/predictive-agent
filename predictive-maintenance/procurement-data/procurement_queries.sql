-- Useful SQL queries for the procurement system
-- Run these after setting up the procurement schema and populating data

-- 1. View stock status vs maintenance needs
SELECT 
    part_number,
    part_description,
    current_stock,
    total_maintenance_need,
    stock_after_maintenance,
    stock_status,
    unit_cost,
    supplier
FROM procurement.stock_vs_maintenance
ORDER BY stock_status, part_number;

-- 2. Parts that need immediate reordering
SELECT 
    part_number,
    part_description,
    current_stock,
    reorder_point,
    unit_cost,
    supplier
FROM procurement.parts_stock
WHERE current_stock < reorder_point
ORDER BY (reorder_point - current_stock) DESC;

-- 3. Parts with insufficient stock for maintenance
SELECT 
    ps.part_number,
    ps.part_description,
    ps.current_stock,
    COALESCE(SUM(sp.quantity), 0) as maintenance_need,
    ps.current_stock - COALESCE(SUM(sp.quantity), 0) as shortfall,
    ps.unit_cost,
    ps.supplier
FROM procurement.parts_stock ps
LEFT JOIN maintenance.service_parts sp ON ps.part_number = sp.part
GROUP BY ps.part_number, ps.part_description, ps.current_stock, ps.unit_cost, ps.supplier
HAVING ps.current_stock < COALESCE(SUM(sp.quantity), 0)
ORDER BY shortfall DESC;

-- 4. Total inventory value by supplier
SELECT 
    supplier,
    COUNT(*) as parts_count,
    SUM(current_stock * unit_cost) as total_inventory_value,
    AVG(unit_cost) as avg_unit_cost
FROM procurement.parts_stock
GROUP BY supplier
ORDER BY total_inventory_value DESC;

-- 5. Stock levels by part category
SELECT 
    CASE 
        WHEN part_number LIKE 'BEAR%' THEN 'Bearings'
        WHEN part_number LIKE 'SEAL%' THEN 'Seals'
        WHEN part_number LIKE 'GREASE%' THEN 'Lubricants'
        WHEN part_number LIKE 'OIL%' THEN 'Oils'
        WHEN part_number LIKE 'FILTER%' THEN 'Filters'
        WHEN part_number LIKE 'GASKET%' THEN 'Gaskets'
        WHEN part_number LIKE 'MOTOR%' THEN 'Motors'
        WHEN part_number LIKE 'PUMP%' THEN 'Pumps'
        WHEN part_number LIKE 'VALVE%' THEN 'Valves'
        ELSE 'Other'
    END as part_category,
    COUNT(*) as parts_count,
    SUM(current_stock) as total_stock,
    AVG(current_stock) as avg_stock,
    SUM(current_stock * unit_cost) as total_value
FROM procurement.parts_stock
GROUP BY 
    CASE 
        WHEN part_number LIKE 'BEAR%' THEN 'Bearings'
        WHEN part_number LIKE 'SEAL%' THEN 'Seals'
        WHEN part_number LIKE 'GREASE%' THEN 'Lubricants'
        WHEN part_number LIKE 'OIL%' THEN 'Oils'
        WHEN part_number LIKE 'FILTER%' THEN 'Filters'
        WHEN part_number LIKE 'GASKET%' THEN 'Gaskets'
        WHEN part_number LIKE 'MOTOR%' THEN 'Motors'
        WHEN part_number LIKE 'PUMP%' THEN 'Pumps'
        WHEN part_number LIKE 'VALVE%' THEN 'Valves'
        ELSE 'Other'
    END
ORDER BY total_value DESC;

-- 6. Maintenance impact on inventory
SELECT 
    sm.unique_id,
    sm.service_description,
    COUNT(sp.part) as parts_needed,
    SUM(sp.quantity) as total_quantity,
    SUM(sp.quantity * ps.unit_cost) as estimated_cost
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
JOIN procurement.parts_stock ps ON sp.part = ps.part_number
GROUP BY sm.unique_id, sm.service_description
ORDER BY estimated_cost DESC;

-- 7. Critical stock alerts
SELECT 
    'LOW_STOCK' as alert_type,
    part_number,
    part_description,
    current_stock,
    minimum_stock,
    'Current stock below minimum' as message
FROM procurement.parts_stock
WHERE current_stock < minimum_stock

UNION ALL

SELECT 
    'REORDER_NEEDED' as alert_type,
    part_number,
    part_description,
    current_stock,
    reorder_point,
    'Stock below reorder point' as message
FROM procurement.parts_stock
WHERE current_stock < reorder_point

UNION ALL

SELECT 
    'INSUFFICIENT_FOR_MAINTENANCE' as alert_type,
    ps.part_number,
    ps.part_description,
    ps.current_stock,
    COALESCE(SUM(sp.quantity), 0) as required_quantity,
    'Insufficient stock for planned maintenance' as message
FROM procurement.parts_stock ps
LEFT JOIN maintenance.service_parts sp ON ps.part_number = sp.part
GROUP BY ps.part_number, ps.part_description, ps.current_stock
HAVING ps.current_stock < COALESCE(SUM(sp.quantity), 0)

ORDER BY alert_type, part_number;

-- 8. Cost analysis for maintenance planning
SELECT 
    sm.unique_id,
    sm.service_description,
    COUNT(DISTINCT sp.part) as unique_parts,
    SUM(sp.quantity) as total_parts_needed,
    SUM(sp.quantity * ps.unit_cost) as total_cost,
    CASE 
        WHEN SUM(sp.quantity) <= ps.current_stock THEN 'IN_STOCK'
        WHEN ps.current_stock > 0 THEN 'PARTIAL_STOCK'
        ELSE 'OUT_OF_STOCK'
    END as availability_status
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
JOIN procurement.parts_stock ps ON sp.part = ps.part_number
GROUP BY sm.unique_id, sm.service_description, ps.current_stock
ORDER BY total_cost DESC;
