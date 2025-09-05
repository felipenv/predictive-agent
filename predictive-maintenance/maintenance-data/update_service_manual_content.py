#!/usr/bin/env python3
"""
Script to update existing service manual records with full content.
This script only updates the service_manual column without deleting existing data.
"""

import os
from pathlib import Path
import psycopg2

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
    
    return service_description, content

def update_service_manual_content(host, database, user, password, port=5432):
    """Update existing records with service manual content."""
    
    service_manuals_dir = Path("service-manuals")
    
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
        
        # Check existing data
        cursor.execute("SELECT COUNT(*) FROM maintenance.service_manual")
        existing_count = cursor.fetchone()[0]
        print(f"Found {existing_count} existing service manual records")
        
        if existing_count == 0:
            print("No existing data found. Please run the full ingest_data.py script first.")
            return
        
        # Update each record with service manual content
        updated_count = 0
        for unique_id in range(1, 101):
            manual_file = service_manuals_dir / f"{unique_id}_manual.md"
            
            if manual_file.exists():
                # Read service manual content
                service_description, service_manual_content = read_service_manual(manual_file)
                
                # Update the record
                cursor.execute("""
                    UPDATE maintenance.service_manual 
                    SET service_manual = %s
                    WHERE unique_id = %s
                """, (service_manual_content, unique_id))
                
                if cursor.rowcount > 0:
                    updated_count += 1
                    print(f"Updated service manual for equipment {unique_id}")
                else:
                    print(f"No record found for equipment {unique_id}")
        
        # Commit the transaction
        conn.commit()
        print(f"\n✅ Successfully updated {updated_count} service manual records!")
        
        # Verify the updates
        print("\nVerifying updates...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(service_manual) as records_with_content,
                COUNT(*) - COUNT(service_manual) as records_without_content
            FROM maintenance.service_manual
        """)
        
        total, with_content, without_content = cursor.fetchone()
        print(f"Total records: {total}")
        print(f"Records with service manual content: {with_content}")
        print(f"Records without service manual content: {without_content}")
        
        # Show sample of updated content
        cursor.execute("""
            SELECT unique_id, service_description, 
                   CASE 
                       WHEN service_manual IS NOT NULL THEN 'Content available'
                       ELSE 'No content'
                   END as content_status
            FROM maintenance.service_manual
            ORDER BY unique_id
            LIMIT 5
        """)
        
        print("\nSample updated records:")
        for row in cursor.fetchall():
            print(f"  Equipment {row[0]}: {row[2]}")
        
        cursor.close()
        conn.close()
        print("\n✅ Database connection closed successfully!")
        
    except Exception as e:
        print(f"❌ Error updating service manual content: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def main():
    """Main function to update service manual content."""
    print("Updating service manual content in existing database records...")
    
    try:
        # Import database configuration
        from db_config import DB_CONFIG
        
        print("\nDatabase configuration:")
        print(f"  Host: {DB_CONFIG['host']}")
        print(f"  Database: {DB_CONFIG['database']}")
        print(f"  User: {DB_CONFIG['user']}")
        print(f"  Port: {DB_CONFIG['port']}")
        
        # Ask for confirmation
        response = input("\nDo you want to proceed with updating service manual content? (y/N): ")
        if response.lower() != 'y':
            print("Update cancelled.")
            return
        
        update_service_manual_content(**DB_CONFIG)
        print("\n🎉 Service manual content update completed successfully!")
        
    except ImportError:
        print("❌ Error: Could not import database configuration.")
        print("Please create db_config.py with your database parameters.")
        print("See db_config.py.example for reference.")
    except Exception as e:
        print(f"❌ Error during update: {e}")
        print("\nTroubleshooting:")
        print("1. Check database connection parameters in db_config.py")
        print("2. Ensure database schema exists and has the service_manual column")
        print("3. Verify user permissions")
        print("4. Check network connectivity to database")

if __name__ == "__main__":
    main()
