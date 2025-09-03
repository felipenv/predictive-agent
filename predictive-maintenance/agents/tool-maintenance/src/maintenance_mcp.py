#!/usr/bin/env python3
"""
MLRun Control Plane (MCP) server for maintenance tools.
Provides tools to interact with maintenance data in PostgreSQL.
"""

import os
import psycopg2
from typing import Annotated, Optional
from fastmcp import FastMCP
from pydantic import Field

# Initialize MCP server
mcp = FastMCP()

# Database connection parameters from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'optimus-postgres.mlrun.svc.cluster.local'),
    'database': os.getenv('DB_NAME', 'optimus'),
    'user': os.getenv('DB_USER', 'optimus'),
    'password': os.getenv('DB_PASSWORD', 'optimus'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

def get_db_connection():
    """Get a database connection using environment variables."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to database: {str(e)}")

@mcp.tool
def get_service_manual(
    unique_id: Annotated[int, Field(description="Unique identifier for the equipment (1-100)")]
) -> str:
    """
    Fetch the service manual and parts list for a given equipment unique_id.
    Returns detailed maintenance information including service description and required parts.
    """
    try:
        if unique_id < 1 or unique_id > 100:
            return f"Error: unique_id must be between 1 and 100. Got: {unique_id}"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get service manual information
        cursor.execute("""
            SELECT 
                sm.unique_id,
                sm.service_description
            FROM maintenance.service_manual sm
            WHERE sm.unique_id = %s
        """, (unique_id,))
        
        service_manual = cursor.fetchone()
        if not service_manual:
            return f"No service manual found for equipment with unique_id: {unique_id}"
        
        # Get parts information
        cursor.execute("""
            SELECT 
                sp.part,
                sp.quantity
            FROM maintenance.service_parts sp
            WHERE sp.equipment_id = %s
            ORDER BY sp.part
        """, (unique_id,))
        
        parts = cursor.fetchall()
        
        # Format the response
        result = f"## Service Manual for Equipment {unique_id}\n\n"
        result += f"**Service Description:**\n{service_manual[1]}\n\n"
        
        if parts:
            result += f"**Required Parts ({len(parts)} total):**\n\n"
            result += "| Part Number | Quantity |\n"
            result += "|-------------|----------|\n"
            
            total_quantity = 0
            for part, quantity in parts:
                result += f"| {part} | {quantity} |\n"
                total_quantity += quantity
            
            result += f"\n**Total Parts Needed:** {total_quantity}\n"
        else:
            result += "**Required Parts:** No parts specified for this service.\n"
        
        cursor.close()
        conn.close()
        
        return result
        
    except Exception as e:
        return f"Error fetching service manual: {str(e)}"

@mcp.tool
def list_equipment_ids() -> str:
    """
    List all available equipment IDs that have service manuals.
    Returns a summary of equipment IDs and their service descriptions.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                sm.unique_id,
                sm.service_description,
                COUNT(sp.part) as parts_count,
                SUM(sp.quantity) as total_quantity
            FROM maintenance.service_manual sm
            LEFT JOIN maintenance.service_parts sp ON sm.unique_id = sp.equipment_id
            GROUP BY sm.unique_id, sm.service_description
            ORDER BY sm.unique_id
        """)
        
        equipment_list = cursor.fetchall()
        
        if not equipment_list:
            return "No equipment found in the database."
        
        result = f"## Available Equipment ({len(equipment_list)} total)\n\n"
        result += "| ID | Parts Count | Total Quantity | Service Description |\n"
        result += "|----|-------------|----------------|---------------------|\n"
        
        for unique_id, description, parts_count, total_quantity in equipment_list:
            # Truncate description if too long
            short_desc = description[:60] + "..." if len(description) > 60 else description
            result += f"| {unique_id} | {parts_count or 0} | {total_quantity or 0} | {short_desc} |\n"
        
        cursor.close()
        conn.close()
        
        return result
        
    except Exception as e:
        return f"Error listing equipment: {str(e)}"

@mcp.tool
def get_equipment_parts(
    equipment_id: Annotated[int, Field(description="Equipment ID (unique_id) to get parts for")]
) -> str:
    """
    Get all parts required for maintenance of a specific equipment.
    Returns detailed parts list with quantities for the given equipment_id.
    """
    try:
        if equipment_id < 1 or equipment_id > 100:
            return f"Error: equipment_id must be between 1 and 100. Got: {equipment_id}"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First check if the equipment exists
        cursor.execute("""
            SELECT unique_id, service_description
            FROM maintenance.service_manual
            WHERE unique_id = %s
        """, (equipment_id,))
        
        equipment = cursor.fetchone()
        if not equipment:
            return f"No equipment found with ID: {equipment_id}"
        
        # Get all parts for this equipment
        cursor.execute("""
            SELECT 
                sp.part,
                sp.quantity
            FROM maintenance.service_parts sp
            WHERE sp.equipment_id = %s
            ORDER BY sp.part
        """, (equipment_id,))
        
        parts = cursor.fetchall()
        
        result = f"## Parts List for Equipment {equipment_id}\n\n"
        result += f"**Service Description:** {equipment[1]}\n\n"
        
        if parts:
            result += f"**Required Parts ({len(parts)} total):**\n\n"
            result += "| Part Number | Quantity |\n"
            result += "|-------------|----------|\n"
            
            total_quantity = 0
            for part, quantity in parts:
                result += f"| {part} | {quantity} |\n"
                total_quantity += quantity
            
            result += f"\n**Total Parts Needed:** {total_quantity}\n"
        else:
            result += "**Required Parts:** No parts specified for this equipment.\n"
        
        cursor.close()
        conn.close()
        
        return result
        
    except Exception as e:
        return f"Error getting equipment parts: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000, path="/")
