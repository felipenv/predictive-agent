#!/usr/bin/env python3
"""
Script to populate the procurement.parts_stock table with realistic inventory data.
This script reads the existing maintenance parts and creates stock entries with realistic quantities.
"""

import psycopg2
from psycopg2.extras import execute_values
import random
from pathlib import Path

# Part categories and their typical stock characteristics
PART_CATEGORIES = {
    'BEAR': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (15.0, 150.0)},
    'SEAL': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (5.0, 45.0)},
    'GREASE': {'min_stock': 2, 'reorder_point': 4, 'cost_range': (8.0, 25.0)},
    'OIL': {'min_stock': 2, 'reorder_point': 4, 'cost_range': (12.0, 35.0)},
    'FILTER': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (20.0, 80.0)},
    'GASKET': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (3.0, 18.0)},
    'MOTOR': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (200.0, 800.0)},
    'BELT': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (25.0, 75.0)},
    'PUMP': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (150.0, 500.0)},
    'VALVE': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (80.0, 300.0)},
    'GEAR': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (120.0, 400.0)},
    'HEAT': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (45.0, 180.0)},
    'TUBE': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (30.0, 120.0)},
    'FAN': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (90.0, 250.0)},
    'TOWER': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (500.0, 1500.0)},
    'NOZZLE': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (15.0, 60.0)},
    'DRIFT': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (25.0, 80.0)},
    'FIN': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (8.0, 35.0)},
    'IMPELLER': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (120.0, 450.0)},
    'SHAFT': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (200.0, 600.0)},
    'CHAIN': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (40.0, 120.0)},
    'SPROCKET': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (35.0, 95.0)},
    'ACTUATOR': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (250.0, 800.0)},
    'POSITIONER': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (180.0, 500.0)},
    'ELEMENT': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (15.0, 65.0)},
    'HOUSING': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (45.0, 150.0)},
    'SWITCH': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (25.0, 80.0)},
    'RELAY': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (18.0, 55.0)},
    'FUSE': {'min_stock': 2, 'reorder_point': 4, 'cost_range': (5.0, 25.0)},
    'LUBRICANT': {'min_stock': 1, 'reorder_point': 3, 'cost_range': (12.0, 40.0)},
    'PINION': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (95.0, 280.0)},
    'RACK': {'min_stock': 0, 'reorder_point': 2, 'cost_range': (120.0, 350.0)}
}

# Supplier names for variety
SUPPLIERS = [
    'Industrial Supply Co.',
    'Maintenance Parts Inc.',
    'Quality Bearings Ltd.',
    'Tech Components Corp.',
    'Reliable Parts Supply',
    'Precision Engineering',
    'Global Industrial',
    'Maintenance Solutions',
    'Parts Warehouse',
    'Industrial Equipment Co.'
]

def get_part_category(part_number):
    """Extract part category from part number."""
    for category in PART_CATEGORIES.keys():
        if part_number.startswith(category):
            return category
    return 'GENERIC'

def generate_stock_data(parts_list):
    """Generate realistic stock data for parts."""
    stock_data = []
    
    for part in parts_list:
        part_number = part.strip()
        if not part_number:
            continue
            
        category = get_part_category(part_number)
        
        if category in PART_CATEGORIES:
            config = PART_CATEGORIES[category]
            min_stock = config['min_stock']
            reorder_point = config['reorder_point']
            cost_range = config['cost_range']
            
            # Generate realistic current stock (sometimes below reorder point)
            if random.random() < 0.9:  # 90% chance of low stock
                current_stock = random.randint(0, reorder_point - 1)
            else:
                current_stock = random.randint(reorder_point, reorder_point * 3)
            
            # Generate unit cost
            unit_cost = round(random.uniform(*cost_range), 2)
            
            # Select random supplier
            supplier = random.choice(SUPPLIERS)
            
            # Create part description
            part_description = f"{category.lower().title()} component for maintenance"
            
            stock_data.append((
                part_number,
                part_description,
                current_stock,
                min_stock,
                reorder_point,
                unit_cost,
                supplier
            ))
        else:
            # Generic part configuration
            current_stock = random.randint(5, 50)
            min_stock = 5
            reorder_point = 15
            unit_cost = round(random.uniform(10.0, 100.0), 2)
            supplier = random.choice(SUPPLIERS)
            part_description = "General maintenance component"
            
            stock_data.append((
                part_number,
                part_description,
                current_stock,
                min_stock,
                reorder_point,
                unit_cost,
                supplier
            ))
    
    return stock_data

def populate_procurement_table(host, database, user, password, port=5432):
    """Populate the procurement.parts_stock table."""
    
    db_params = {
        'host': host,
        'database': database,
        'user': user,
        'password': password,
        'port': port
    }
    
    try:
        # Connect to database
        print(f"Connecting to PostgreSQL database: {database} on {host}:{port}")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Get all unique parts from maintenance.service_parts
        print("Fetching parts from maintenance.service_parts...")
        cursor.execute("SELECT DISTINCT part FROM maintenance.service_parts ORDER BY part")
        parts = [row[0] for row in cursor.fetchall()]
        
        print(f"Found {len(parts)} unique parts")
        
        # Generate stock data
        print("Generating realistic stock data...")
        stock_data = generate_stock_data(parts)
        
        # Clear existing data
        print("Clearing existing parts_stock data...")
        cursor.execute("DELETE FROM procurement.parts_stock")
        
        # Insert stock data using execute_values for efficiency
        print(f"Inserting {len(stock_data)} stock records...")
        stock_query = """
            INSERT INTO procurement.parts_stock 
            (part_number, part_description, current_stock, minimum_stock, reorder_point, unit_cost, supplier) 
            VALUES %s
        """
        execute_values(cursor, stock_query, stock_data)
        
        # Commit the transaction
        conn.commit()
        print("✅ Stock data inserted successfully!")
        
        # Verify the data
        print("\nVerifying data...")
        cursor.execute("SELECT COUNT(*) FROM procurement.parts_stock")
        stock_count = cursor.fetchone()[0]
        print(f"Parts in stock: {stock_count}")
        
        # Show sample data
        cursor.execute("""
            SELECT part_number, current_stock, reorder_point, unit_cost, supplier
            FROM procurement.parts_stock
            ORDER BY part_number
            LIMIT 10
        """)
        
        print("\nSample stock data:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} in stock, reorder at {row[2]}, ${row[3]}, {row[4]}")
        
        # Check stock status view
        cursor.execute("""
            SELECT COUNT(*) as count, stock_status
            FROM procurement.stock_vs_maintenance
            GROUP BY stock_status
            ORDER BY stock_status
        """)
        
        print("\nStock status summary:")
        for row in cursor.fetchall():
            print(f"  {row[1]}: {row[0]} parts")
        
        cursor.close()
        conn.close()
        print("\n✅ Database connection closed successfully!")
        
    except Exception as e:
        print(f"❌ Error populating procurement table: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def main():
    """Main function to populate procurement data."""
    print("Populating procurement.parts_stock table...")
    
    try:
        # Import database configuration
        from db_config import DB_CONFIG
        
        print(f"Database: {DB_CONFIG['database']} on {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        populate_procurement_table(**DB_CONFIG)
        print("\n🎉 Procurement data population completed successfully!")
        print("\nNext steps:")
        print("1. Check the stock_vs_maintenance view for insights")
        print("2. Query parts that need reordering")
        print("3. Analyze stock levels vs maintenance needs")
        
    except ImportError:
        print("❌ Error: Could not import database configuration.")
        print("Please create db_config.py with your database parameters.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
