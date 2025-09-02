#!/usr/bin/env python3
"""
Script to check the current database schema and help diagnose issues.
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_schema(host, database, user, password, port=5432):
    """Check the current database schema."""
    
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
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if maintenance schema exists
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'maintenance'
        """)
        
        if cursor.fetchone():
            print("✅ Maintenance schema exists")
        else:
            print("❌ Maintenance schema does not exist")
            return
        
        # Check service_manual table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'maintenance' 
            AND table_name = 'service_manual'
            ORDER BY ordinal_position
        """)
        
        print("\n📋 service_manual table structure:")
        columns = cursor.fetchall()
        if columns:
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  {col['column_name']}: {col['data_type']} {nullable}{default}")
        else:
            print("  ❌ Table does not exist")
        
        # Check service_parts table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'maintenance' 
            AND table_name = 'service_parts'
            ORDER BY ordinal_position
        """)
        
        print("\n📋 service_parts table structure:")
        columns = cursor.fetchall()
        if columns:
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  {col['column_name']}: {col['data_type']} {nullable}{default}")
        else:
            print("  ❌ Table does not exist")
        
        # Check for any data
        cursor.execute("SELECT COUNT(*) as count FROM maintenance.service_manual")
        manual_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM maintenance.service_parts")
        parts_count = cursor.fetchone()['count']
        
        print(f"\n📊 Current data:")
        print(f"  service_manual: {manual_count} records")
        print(f"  service_parts: {parts_count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking schema: {e}")

def main():
    """Main function to check database schema."""
    print("Checking database schema...")
    
    try:
        # Import database configuration
        from db_config import DB_CONFIG
        
        print(f"Database: {DB_CONFIG['database']} on {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        check_schema(**DB_CONFIG)
        
    except ImportError:
        print("❌ Error: Could not import database configuration.")
        print("Please create db_config.py with your database parameters.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
