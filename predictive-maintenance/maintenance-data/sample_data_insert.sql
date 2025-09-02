-- Sample data insertion for maintenance schema
-- This shows how to populate the tables with the generated service manuals and parts

-- First, insert service manuals
INSERT INTO maintenance.service_manual (unique_id, service_description) VALUES
(1, 'Turbine Engine GT-1000 - Regular maintenance including bearing replacement, seal inspection, and lubrication'),
(2, 'Compressor CP-2000 - Scheduled maintenance with motor inspection, belt replacement, and coupling check'),
(3, 'Pump Assembly PA-3000 - Maintenance service including impeller inspection, shaft alignment, and bearing replacement'),
(25, 'Heat Exchanger HE-5000 - Thermal system maintenance with tube inspection, fin cleaning, and gasket replacement'),
(50, 'Cooling Tower CT-1000 - Cooling system maintenance including fan inspection, drift elimination, and nozzle cleaning');

-- Then, insert parts with quantities
-- Equipment ID 1 (Turbine Engine)
INSERT INTO maintenance.service_parts (equipment_id, part, quantity) VALUES
(1, 'BEAR-001-01', 3),
(1, 'SEAL-001-02', 2),
(1, 'OIL-FILTER-001-03', 1),
(1, 'GREASE-001-04', 3),
(1, 'GREASE-001-01', 3),
(1, 'OIL-001-01', 2),
(1, 'FILTER-001-01', 1),
(1, 'GASKET-001-01', 2);

-- Equipment ID 25 (Heat Exchanger)
INSERT INTO maintenance.service_parts (equipment_id, part, quantity) VALUES
(25, 'HEAT-025-01', 2),
(25, 'TUBE-025-02', 2),
(25, 'FIN-025-03', 2),
(25, 'GASKET-025-04', 2),
(25, 'GREASE-025-01', 3),
(25, 'OIL-025-01', 2),
(25, 'FILTER-025-01', 1),
(25, 'GASKET-025-01', 2);

-- Equipment ID 50 (Cooling Tower)
INSERT INTO maintenance.service_parts (equipment_id, part, quantity) VALUES
(50, 'TOWER-050-01', 3),
(50, 'FAN-050-02', 3),
(50, 'DRIFT-050-03', 3),
(50, 'NOZZLE-050-04', 3),
(50, 'GREASE-050-01', 4),
(50, 'OIL-050-01', 3),
(50, 'FILTER-050-01', 1),
(50, 'GASKET-050-01', 1);

-- Query examples to verify the data
-- Get all parts for a specific equipment
SELECT 
    sm.unique_id,
    sm.service_description,
    sp.part,
    sp.quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
WHERE sm.unique_id = 25
ORDER BY sp.part;

-- Get total parts count and value for each equipment
SELECT 
    sm.unique_id,
    sm.service_description,
    COUNT(sp.part) as total_parts,
    SUM(sp.quantity) as total_quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
GROUP BY sm.unique_id, sm.service_description
ORDER BY sm.unique_id;

-- Search for equipment that needs a specific part
SELECT 
    sm.unique_id,
    sm.service_description,
    sp.quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
WHERE sp.part LIKE '%GREASE%'
ORDER BY sm.unique_id;
