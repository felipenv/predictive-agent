# Maintenance Data System

This system generates service manuals and parts lists for equipment IDs 1-100, and provides tools to ingest this data into a PostgreSQL database.

## 📁 File Structure

```
maintenance-data/
├── database-config.sql          # PostgreSQL schema creation
├── generate_manuals.py          # Script to generate service manuals and parts
├── ingest_data.py              # Script to ingest data into PostgreSQL
├── db_config.py                # Database connection configuration
├── requirements.txt             # Python dependencies
├── migrate_schema.sql           # Schema migration script
├── check_schema.py             # Database schema verification script
├── service-manuals/            # Generated service manuals and parts
│   ├── 1_manual.md            # Service manual for equipment 1
│   ├── 1_parts.txt            # Parts list for equipment 1
│   ├── 2_manual.md            # Service manual for equipment 2
│   ├── 2_parts.txt            # Parts list for equipment 2
│   └── ...                     # ... up to equipment 100
├── procurement-data/            # Procurement and inventory management
│   ├── README.md               # Procurement system documentation
│   ├── procurement-config.sql  # Procurement schema creation
│   ├── populate_procurement.py # Script to populate parts stock
│   └── procurement_queries.sql # Useful procurement queries
└── README.md                   # This file
```

## 🚀 Quick Start

### 1. Set Up Database Schema

First, create the database schema and tables:

```bash
# Connect to your PostgreSQL database
psql -h your_host -U your_user -d your_database

# Run the schema creation script
\i database-config.sql
```

### 2. Configure Database Connection

Edit `db_config.py` with your database credentials:

```python
DB_CONFIG = {
    'host': 'your_postgresql_host',
    'database': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'port': 5432
}
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Generate Service Manuals and Parts

```bash
python generate_manuals.py
```

This creates 100 pairs of files:
- `{id}_manual.md` - Service manual in markdown format
- `{id}_parts.txt` - Parts list with quantities (format: `part,quantity`)

### 5. Ingest Data into Database

```bash
python ingest_data.py
```

This script will:
- Connect to your PostgreSQL database
- Clear existing data (optional)
- Insert all service manuals and parts
- Verify the data insertion
- Show summary statistics

## 🗄️ Database Schema

### `maintenance.service_manual`
- `id` - Auto-incrementing primary key
- `unique_id` - Equipment identifier (matches MLRun dataset)
- `service_description` - Description of maintenance service

### `maintenance.service_parts`
- `id` - Auto-incrementing primary key
- `equipment_id` - Foreign key to `service_manual.unique_id`
- `part` - Part number or description
- `quantity` - Quantity of parts required

## 📊 Data Format

### Service Manuals
- **Format**: Markdown files
- **Content**: Equipment overview, maintenance schedule, procedures, safety checks
- **Equipment Types**: 10 different types rotating through IDs 1-100

### Parts Lists
- **Format**: Text files with `part,quantity` pairs
- **Example**:
  ```
  BEAR-001-01,3
  SEAL-001-02,2
  GREASE-001-01,3
  ```

## 🔍 Query Examples

### Get all parts for specific equipment
```sql
SELECT sp.part, sp.quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
WHERE sm.unique_id = 25;
```

### Find equipment needing specific parts
```sql
SELECT sm.unique_id, sm.service_description, sp.quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
WHERE sp.part LIKE '%BEAR%';
```

### Get maintenance summary
```sql
SELECT 
    sm.unique_id,
    COUNT(sp.part) as total_parts,
    SUM(sp.quantity) as total_quantity
FROM maintenance.service_manual sm
JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
GROUP BY sm.unique_id
ORDER BY sm.unique_id;
```

## 🔧 Customization

### Equipment Types
Edit `generate_manuals.py` to modify:
- Equipment types and manufacturers
- Part categories and quantities
- Maintenance schedules and procedures

### Database Schema
Modify `database-config.sql` to:
- Add new tables or columns
- Change indexes or constraints
- Modify permissions

## 🚨 Troubleshooting

### Common Issues

1. **Connection Error**: Check database credentials in `db_config.py`
2. **Schema Error**: Ensure `database-config.sql` was run first
3. **Permission Error**: Verify user has CREATE/INSERT permissions
4. **Import Error**: Install dependencies with `pip install -r requirements.txt`

### Verification

After ingestion, verify data with:
```sql
SELECT COUNT(*) FROM maintenance.service_manual;      -- Should be 100
SELECT COUNT(*) FROM maintenance.service_parts;       -- Should be ~800
```

## 🔗 Integration with MLRun

This system is designed to work with your MLRun predictive maintenance workflows:

1. **Link predictions** by `unique_id` to get maintenance requirements
2. **Query parts** needed for predicted maintenance
3. **Generate work orders** with required parts and quantities
4. **Track inventory** against maintenance needs

## 🛒 Procurement System

The `procurement-data/` folder contains a complete inventory management system:

1. **Stock tracking** for all maintenance parts
2. **Reorder points** and procurement alerts
3. **Cost analysis** for maintenance planning
4. **Supplier management** for sourcing
5. **Integration** with maintenance requirements

See `procurement-data/README.md` for detailed setup and usage instructions.

## 📝 Notes

- All files are regenerated when running `generate_manuals.py`
- Database insertion clears existing data by default
- Parts quantities are realistic based on equipment type
- Service manuals are one-page markdown for easy reading
