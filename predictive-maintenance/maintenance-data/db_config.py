#!/usr/bin/env python3
"""
Database configuration file for maintenance data ingestion.
Update these parameters to match your PostgreSQL environment.
"""

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',           # Your PostgreSQL host
    'database': 'optimus',         # Your database name  
    'user': 'optimus',       # Your username
    'password': 'optimus',   # Your password
    'port': 31877                   # Your PostgreSQL port
}

# Alternative: Use environment variables for security
# import os
# DB_CONFIG = {
#     'host': os.getenv('DB_HOST', 'localhost'),
#     'database': os.getenv('DB_NAME', 'optimus'),
#     'user': os.getenv('DB_USER', 'your_username'),
#     'password': os.getenv('DB_PASSWORD', 'your_password'),
#     'port': int(os.getenv('DB_PORT', '5432'))
# }
