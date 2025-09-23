#!/usr/bin/env python3
"""
MLRun Control Plane (MCP) server for procurement tools.
Provides tools to check parts stock and generate procurement orders.
"""

import os
import psycopg2
import re
from typing import Annotated, List, Dict, Tuple
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

def parse_parts_list(parts_text: str) -> List[Tuple[str, int]]:
    """
    Parse parts list from maintenance tool output.
    Handles various formats including bullet points, dashes, and different spacing.
    Expected formats:
    - "PART-XXX-XX: X [quantity_word]" (units, pieces, items, parts, etc.)
    - "PART-XXX-XX: X" (just number, no quantity word)
    - "- PART-XXX-XX: X [quantity_word]"
    - "• PART-XXX-XX: X [quantity_word]"
    - "PART-XXX-XX - X [quantity_word]"
    - "PART-XXX-XX - X" (just number, no quantity word)
    Returns list of (part_number, quantity) tuples.
    """
    parts = []
    lines = parts_text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or ':' not in line:
            continue
        
        # Remove bullet points, dashes, and other list markers
        # Handle: "- ", "• ", "* ", "1. ", "1) ", etc.
        line = re.sub(r'^[\s]*[-•*]\s*', '', line)  # Remove bullet points
        line = re.sub(r'^[\s]*\d+[\.\)]\s*', '', line)  # Remove numbered lists
        line = line.strip()
        
        # Match pattern: "PART-XXX-XX: X [quantity_word]" (with flexible spacing)
        # Handles various quantity words: units, pieces, items, parts, etc.
        match = re.match(r'^([A-Z]+-\d{3}-\d{2})\s*:\s*(\d+)(?:\s*[a-zA-Z]+)?$', line, re.IGNORECASE)
        if match:
            part_number = match.group(1)
            quantity = int(match.group(2))
            parts.append((part_number, quantity))
        else:
            # Try alternative format: "PART-XXX-XX - X [quantity_word]" (with dash separator)
            match = re.match(r'^([A-Z]+-\d{3}-\d{2})\s*-\s*(\d+)(?:\s*[a-zA-Z]+)?$', line, re.IGNORECASE)
            if match:
                part_number = match.group(1)
                quantity = int(match.group(2))
                parts.append((part_number, quantity))
    
    return parts

@mcp.tool
def check_parts_stock(
    parts_list: Annotated[str, Field(description="Parts list from maintenance tool (format: 'PART-XXX-XX: X units' per line)")]
) -> str:
    """
    Check stock levels for parts from maintenance tool output.
    Returns stock status and identifies parts that need ordering.
    """
    try:
        # Parse the parts list
        parts_needed = parse_parts_list(parts_list)
        if not parts_needed:
            return "Error: No valid parts found in the input. Expected format: 'PART-XXX-XX: X units' per line"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        result = "## Parts Stock Analysis\n\n"
        result += f"**Parts to check:** {len(parts_needed)}\n\n"
        
        # Check stock for each part
        stock_status = []
        for part_number, quantity_needed in parts_needed:
            cursor.execute("""
                SELECT 
                    part_number,
                    part_description,
                    current_stock,
                    minimum_stock,
                    reorder_point,
                    unit_cost,
                    supplier
                FROM procurement.parts_stock
                WHERE part_number = %s
            """, (part_number,))
            
            stock_info = cursor.fetchone()
            
            if stock_info:
                part_num, description, current_stock, min_stock, reorder_point, unit_cost, supplier = stock_info
                
                # Determine stock status
                if current_stock >= quantity_needed:
                    status = "IN STOCK"
                    needs_order = False
                elif current_stock >= reorder_point:
                    status = "LOW STOCK"
                    needs_order = True
                else:
                    status = "OUT OF STOCK"
                    needs_order = True
                
                stock_status.append({
                    'part_number': part_num,
                    'description': description,
                    'quantity_needed': quantity_needed,
                    'current_stock': current_stock,
                    'status': status,
                    'needs_order': needs_order,
                    'unit_cost': unit_cost,
                    'supplier': supplier
                })
            else:
                # Part not found in procurement system
                stock_status.append({
                    'part_number': part_number,
                    'description': 'Not in procurement system',
                    'quantity_needed': quantity_needed,
                    'current_stock': 0,
                    'status': 'NOT FOUND',
                    'needs_order': True,
                    'unit_cost': 0,
                    'supplier': 'Unknown'
                })
        
        # Create stock status table
        result += "| Part Number | Description | Needed | Current | Status | Unit Cost | Supplier |\n"
        result += "|-------------|-------------|--------|---------|--------|-----------|----------|\n"
        
        for item in stock_status:
            result += f"| {item['part_number']} | {item['description'][:30]}... | {item['quantity_needed']} | {item['current_stock']} | {item['status']} | ${item['unit_cost']:.2f} | {item['supplier']} |\n"
        
        cursor.close()
        conn.close()
        
        return result
        
    except Exception as e:
        return f"Error checking parts stock: {str(e)}"

@mcp.tool
def generate_procurement_order(
    parts_list: Annotated[str, Field(description="Parts list from maintenance tool (format: 'PART-XXX-XX: X units' per line)")]
) -> str:
    """
    Generate a procurement order for parts that are out of stock or below reorder point.
    Returns a formatted procurement order in markdown.
    """
    try:
        # Parse the parts list
        parts_needed = parse_parts_list(parts_list)
        if not parts_needed:
            return "Error: No valid parts found in the input. Expected format: 'PART-XXX-XX: X units' per line"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check stock and identify parts to order
        parts_to_order = []
        total_cost = 0
        
        for part_number, quantity_needed in parts_needed:
            cursor.execute("""
                SELECT 
                    part_number,
                    part_description,
                    current_stock,
                    minimum_stock,
                    reorder_point,
                    unit_cost,
                    supplier
                FROM procurement.parts_stock
                WHERE part_number = %s
            """, (part_number,))
            
            stock_info = cursor.fetchone()
            
            if stock_info:
                part_num, description, current_stock, min_stock, reorder_point, unit_cost, supplier = stock_info
                
                # Calculate how many to order
                if current_stock < quantity_needed:
                    # Need to order enough to cover the requirement
                    order_quantity = quantity_needed - current_stock
                    # Add buffer to reach reorder point
                    if current_stock < reorder_point:
                        order_quantity = max(order_quantity, reorder_point - current_stock + 2)
                    
                    parts_to_order.append({
                        'part_number': part_num,
                        'description': description,
                        'quantity_needed': quantity_needed,
                        'current_stock': current_stock,
                        'order_quantity': order_quantity,
                        'unit_cost': unit_cost,
                        'total_cost': order_quantity * unit_cost,
                        'supplier': supplier
                    })
                    total_cost += order_quantity * unit_cost
            else:
                # Part not found - estimate order quantity
                order_quantity = quantity_needed + 2  # Add buffer
                parts_to_order.append({
                    'part_number': part_number,
                    'description': 'Not in procurement system',
                    'quantity_needed': quantity_needed,
                    'current_stock': 0,
                    'order_quantity': order_quantity,
                    'unit_cost': 0,
                    'total_cost': 0,
                    'supplier': 'Unknown'
                })
        
        if not parts_to_order:
            return "## Procurement Order Status\n\nAll parts are in stock. No procurement order needed."
        
        # Debug: Log the number of parts being processed
        print(f"DEBUG: Processing {len(parts_to_order)} parts for procurement order")
        
        # Generate procurement order
        result = "## Procurement Order\n\n"
        result += f"**Order Date:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        result += f"**Total Parts to Order:** {len(parts_to_order)}\n\n"
        
        # ALWAYS include the parts table - this is required output
        result += "**Parts to Order:**\n\n"
        result += "| Part Number | Description | Needed | Current | Order Qty | Unit Cost | Total Cost | Supplier |\n"
        result += "|-------------|-------------|--------|---------|-----------|-----------|------------|----------|\n"
        
        for item in parts_to_order:
            if item['unit_cost'] > 0:
                result += f"| {item['part_number']} | {item['description'][:25]}... | {item['quantity_needed']} | {item['current_stock']} | {item['order_quantity']} | ${item['unit_cost']:.2f} | ${item['total_cost']:.2f} | {item['supplier']} |\n"
            else:
                result += f"| {item['part_number']} | {item['description'][:25]}... | {item['quantity_needed']} | {item['current_stock']} | {item['order_quantity']} | N/A | N/A | {item['supplier']} |\n"
        
        # Summary
        result += f"\n**Order Summary:**\n"
        result += f"- **Total Parts:** {len(parts_to_order)}\n"
        result += f"- **Estimated Total Cost:** ${total_cost:.2f}\n"
        result += f"- **Priority:** High (Maintenance Required)\n\n"
        
        # Action items
        result += "**Next Steps:**\n"
        result += "1. Review parts list and quantities\n"
        result += "2. Contact suppliers for availability and pricing\n"
        result += "3. Place orders for approved parts\n"
        result += "4. Update inventory system upon receipt\n"
        
        cursor.close()
        conn.close()
        
        # Final validation: Ensure the table is always included
        if "| Part Number | Description |" not in result:
            result += "\n**ERROR: Table format missing from output!**\n"
            result += "| Part Number | Description | Needed | Current | Order Qty | Unit Cost | Total Cost | Supplier |\n"
            result += "|-------------|-------------|--------|---------|-----------|-----------|------------|----------|\n"
            for item in parts_to_order:
                result += f"| {item['part_number']} | {item['description'][:25]}... | {item['quantity_needed']} | {item['current_stock']} | {item['order_quantity']} | ${item.get('unit_cost', 0):.2f} | ${item.get('total_cost', 0):.2f} | {item.get('supplier', 'Unknown')} |\n"
        
        return result
        
    except Exception as e:
        return f"Error generating procurement order: {str(e)}"

@mcp.tool
def get_stock_summary() -> str:
    """
    Get a summary of all parts in the procurement system.
    Shows current stock levels, low stock items, and out-of-stock items.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get overall stock summary
        cursor.execute("""
            SELECT 
                COUNT(*) as total_parts,
                SUM(CASE WHEN current_stock > reorder_point THEN 1 ELSE 0 END) as in_stock,
                SUM(CASE WHEN current_stock <= reorder_point AND current_stock > minimum_stock THEN 1 ELSE 0 END) as low_stock,
                SUM(CASE WHEN current_stock <= minimum_stock THEN 1 ELSE 0 END) as out_of_stock,
                SUM(current_stock * unit_cost) as total_inventory_value
            FROM procurement.parts_stock
        """)
        
        summary = cursor.fetchone()
        
        # Get low stock items
        cursor.execute("""
            SELECT 
                part_number,
                part_description,
                current_stock,
                minimum_stock,
                reorder_point,
                unit_cost,
                supplier
            FROM procurement.parts_stock
            WHERE current_stock <= reorder_point
            ORDER BY current_stock ASC
            LIMIT 10
        """)
        
        low_stock_items = cursor.fetchall()
        
        result = "## Procurement Stock Summary\n\n"
        result += f"**Overall Status:**\n"
        result += f"- **Total Parts:** {summary[0]}\n"
        result += f"- **In Stock:** {summary[1]}\n"
        result += f"- **Low Stock:** {summary[2]}\n"
        result += f"- **Out of Stock:** {summary[3]}\n"
        result += f"- **Total Inventory Value:** ${summary[4]:,.2f}\n\n"
        
        if low_stock_items:
            result += "**Low Stock Items (Top 10):**\n\n"
            result += "| Part Number | Description | Current | Min | Reorder | Unit Cost | Supplier |\n"
            result += "|-------------|-------------|---------|-----|---------|-----------|----------|\n"
            
            for item in low_stock_items:
                result += f"| {item[0]} | {item[1][:30]}... | {item[2]} | {item[3]} | {item[4]} | ${item[5]:.2f} | {item[6]} |\n"
        else:
            result += "**Low Stock Items:** None\n"
        
        cursor.close()
        conn.close()
        
        return result
        
    except Exception as e:
        return f"Error getting stock summary: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000, path="/")
