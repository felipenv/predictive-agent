# Procurement Data System

This folder contains the procurement and inventory management system for predictive maintenance parts.

## 📁 File Structure

```
procurement-data/
├── README.md                    # This file
├── procurement-config.sql       # PostgreSQL schema creation for procurement
├── populate_procurement.py      # Script to populate parts_stock table
└── procurement_queries.sql     # Useful SQL queries for procurement analysis
```

## 🚀 Quick Start

### 1. Set Up Procurement Schema

First, create the procurement schema and tables:

```bash
# Connect to your PostgreSQL database
psql -h localhost -U optimus -d optimus -p 31117

# Run the schema creation script
\i procurement-config.sql
```

### 2. Populate Stock Data

Run the Python script to populate the parts_stock table:

```bash
cd procurement-data
python populate_procurement.py
```

This will:
- Read all parts from `maintenance.service_parts`
- Generate realistic stock levels and costs
- Set appropriate reorder points
- Assign suppliers
- Create ~800 stock records

### 3. Analyze Procurement Data

Use the SQL queries in `procurement_queries.sql` to analyze:

- Stock status vs maintenance needs
- Parts requiring reordering
- Inventory value by supplier
- Stock levels by category
- Critical stock alerts

## 🗄️ Database Schema

### `procurement.parts_stock`
- `part_number` - Links to maintenance parts
- `current_stock` - Current inventory quantity
- `minimum_stock` - Safety stock level
- `reorder_point` - When to reorder
- `unit_cost` - Cost per unit
- `supplier` - Preferred supplier
- `last_updated` - Timestamp

### `procurement.stock_vs_maintenance` (View)
Shows stock status compared to maintenance requirements:
- **SUFFICIENT** - Enough stock for maintenance
- **INSUFFICIENT_FOR_MAINTENANCE** - Need more parts
- **LOW_STOCK** - Below minimum levels
- **REORDER** - Below reorder point

## 🔍 Key Features

### Stock Management
- **Realistic stock levels** based on part type
- **Automated reorder points** for procurement alerts
- **Cost tracking** for budget planning
- **Supplier management** for sourcing

### Integration with Maintenance
- **Links parts** from maintenance service requirements
- **Shows stock vs maintenance needs**
- **Identifies shortages** before they cause delays
- **Cost analysis** for maintenance planning

## 📊 Example Queries

### Check Stock Status
```sql
SELECT * FROM procurement.stock_vs_maintenance 
WHERE stock_status = 'INSUFFICIENT_FOR_MAINTENANCE';
```

### Parts Needing Reorder
```sql
SELECT part_number, current_stock, reorder_point, supplier
FROM procurement.parts_stock
WHERE current_stock < reorder_point;
```

### Maintenance Cost Analysis
```sql
SELECT 
    sm.unique_id,
    SUM(sp.quantity * ps.unit_cost) as total_cost
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
JOIN procurement.parts_stock ps ON sp.part = ps.part_number
GROUP BY sm.unique_id
ORDER BY total_cost DESC;
```

## 🔗 MLRun Integration

This system enables:
1. **Predict maintenance needs** with MLRun workflows
2. **Check stock availability** for predicted equipment
3. **Generate procurement orders** for missing parts
4. **Plan maintenance costs** based on inventory
5. **Optimize stock levels** based on ML predictions

## 🚨 Troubleshooting

### Common Issues
1. **Schema Error**: Ensure `procurement-config.sql` was run first
2. **Import Error**: Check database connection in `db_config.py`
3. **Permission Error**: Verify user has CREATE/INSERT permissions

### Verification
After population, verify data with:
```sql
SELECT COUNT(*) FROM procurement.parts_stock;  -- Should be ~800
SELECT COUNT(*) FROM procurement.stock_vs_maintenance;  -- Should be ~800
```
