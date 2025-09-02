#!/usr/bin/env python3
"""
Script to directly ingest service manual and parts data into PostgreSQL database.
This script reads the generated files and inserts data using psycopg2.
"""

import os
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

def read_service_manual(manual_file):
    """Read service manual and extract key information."""
    with open(manual_file, 'r') as f:
        content = f.read()
    
    # Extract equipment type and create service description
    lines = content.split('\n')
    equipment_type = ""
    for line in lines:
        if "**Equipment Type:**" in line:
            equipment_type = line.split("**Equipment Type:**")[1].strip()
            break
    
    # Create a meaningful service description
    service_description = f"{equipment_type} - Comprehensive maintenance service including inspection, lubrication, and parts replacement"
    
    return service_description

def read_parts_list(parts_file):
    """Read parts list and parse part,quantity pairs."""
    parts = []
    with open(parts_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and ',' in line:
                part, quantity = line.split(',')
                parts.append((part.strip(), int(quantity)))
    return parts

def generate_sql_inserts():
    """Generate SQL INSERT statements for all data."""
    
    service_manuals_dir = Path("service-manuals")
    
    # SQL statements
    service_manual_inserts = []
    parts_inserts = []
    
    for unique_id in range(1, 101):
        manual_file = service_manuals_dir / f"{unique_id}_manual.md"
        parts_file = service_manuals_dir / f"{unique_id}_parts.txt"
        
        if manual_file.exists() and parts_file.exists():
            # Read service manual
            service_description = read_service_manual(manual_file)
            service_manual_inserts.append((unique_id, service_description))
            
            # Read parts
            parts = read_parts_list(parts_file)
            for part, quantity in parts:
                parts_inserts.append((unique_id, part, quantity))
    
    return service_manual_inserts, parts_inserts

def insert_data_to_database(host, database, user, password, port=5432):
    """Insert data directly into PostgreSQL database."""
    
    service_manual_inserts, parts_inserts = generate_sql_inserts()
    
    # Database connection parameters
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
        
        # Clear existing data (optional)
        print("Clearing existing data...")
        cursor.execute("DELETE FROM maintenance.service_parts")
        cursor.execute("DELETE FROM maintenance.service_manual")
        
        # Insert service manuals using execute_values for efficiency
        print(f"Inserting {len(service_manual_inserts)} service manuals...")
        service_manual_query = """
            INSERT INTO maintenance.service_manual (unique_id, service_description) 
            VALUES %s
        """
        execute_values(cursor, service_manual_query, service_manual_inserts)
        
        # Insert parts using execute_values for efficiency
        print(f"Inserting {len(parts_inserts)} parts...")
        parts_query = """
            INSERT INTO maintenance.service_parts (equipment_id, part, quantity) 
            VALUES %s
        """
        execute_values(cursor, parts_query, parts_inserts)
        
        # Commit the transaction
        conn.commit()
        print("✅ Data inserted successfully!")
        
        # Verify the data
        print("\nVerifying data...")
        cursor.execute("SELECT COUNT(*) FROM maintenance.service_manual")
        manual_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM maintenance.service_parts")
        parts_count = cursor.fetchone()[0]
        
        print(f"Service manuals: {manual_count}")
        print(f"Parts: {parts_count}")
        
        # Get sample data
        cursor.execute("""
            SELECT 
                sm.unique_id,
                sm.service_description,
                COUNT(sp.part) as total_parts,
                SUM(sp.quantity) as total_quantity
            FROM maintenance.service_manual sm
            JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
            GROUP BY sm.unique_id, sm.service_description
            ORDER BY sm.unique_id
            LIMIT 5
        """)
        
        print("\nSample data summary:")
        for row in cursor.fetchall():
            print(f"  Equipment {row[0]}: {row[3]} total parts needed")
        
        cursor.close()
        conn.close()
        print("\n✅ Database connection closed successfully!")
        
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def main():
    """Main function to ingest data directly into database."""
    print("Direct database ingestion for maintenance data...")
    
    try:
        # Import database configuration
        from db_config import DB_CONFIG
        
        print("\nDatabase configuration:")
        print(f"  Host: {DB_CONFIG['host']}")
        print(f"  Database: {DB_CONFIG['database']}")
        print(f"  User: {DB_CONFIG['user']}")
        print(f"  Port: {DB_CONFIG['port']}")
        
        # Ask for confirmation
        response = input("\nDo you want to proceed with database insertion? (y/N): ")
        if response.lower() != 'y':
            print("Database insertion cancelled.")
            return
        
        insert_data_to_database(**DB_CONFIG)
        print("\n🎉 Data ingestion completed successfully!")
        print("\nNext steps:")
        print("1. Verify the data in your database")
        print("2. Link with your MLRun workflows")
        print("3. Query maintenance requirements for predicted equipment")
        
    except ImportError:
        print("❌ Error: Could not import database configuration.")
        print("Please create db_config.py with your database parameters.")
        print("See db_config.py.example for reference.")
    except Exception as e:
        print(f"❌ Error during data ingestion: {e}")
        print("\nTroubleshooting:")
        print("1. Check database connection parameters in db_config.py")
        print("2. Ensure database schema exists (run database-config.sql first)")
        print("3. Verify user permissions")
        print("4. Check network connectivity to database")

if __name__ == "__main__":
    main()
