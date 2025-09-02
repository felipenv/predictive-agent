#!/usr/bin/env python3
"""
Script to generate service manuals and parts files for equipment IDs 1-100.
This creates pairs of files: {unique_id}_manual.md and {unique_id}_parts.txt
"""

import os
from pathlib import Path

# Equipment types and their characteristics
EQUIPMENT_TYPES = [
    ("Turbine Engine", "GT-1000", "AeroTech Industries"),
    ("Compressor", "CP-2000", "CompTech Systems"),
    ("Pump Assembly", "PA-3000", "PumpWorks Inc"),
    ("Motor Drive", "MD-4000", "MotorTech Solutions"),
    ("Heat Exchanger", "HE-5000", "Thermal Systems"),
    ("Valve Assembly", "VA-6000", "ValveTech Corp"),
    ("Gear Box", "GB-7000", "GearWorks Ltd"),
    ("Control Panel", "CP-8000", "ControlTech"),
    ("Filter System", "FS-9000", "FilterTech"),
    ("Cooling Tower", "CT-1000", "CoolTech Industries")
]

# Common part categories
PART_CATEGORIES = [
    ["BEAR", "SEAL", "OIL-FILTER", "GREASE"],
    ["MOTOR", "BELT", "PULLEY", "COUPLING"],
    ["PUMP", "IMPELLER", "SHAFT", "BEARING"],
    ["DRIVE", "GEAR", "CHAIN", "SPROCKET"],
    ["HEAT", "TUBE", "FIN", "GASKET"],
    ["VALVE", "ACTUATOR", "POSITIONER", "SEAT"],
    ["GEAR", "PINION", "RACK", "LUBRICANT"],
    ["PANEL", "SWITCH", "RELAY", "FUSE"],
    ["FILTER", "ELEMENT", "HOUSING", "GASKET"],
    ["TOWER", "FAN", "DRIFT", "NOZZLE"]
]

def generate_service_manual(unique_id, equipment_type, model, manufacturer):
    """Generate a service manual in markdown format."""
    
    manual_content = f"""# Service Manual - Equipment ID: {unique_id}

## Equipment Overview
**Equipment Type:** {equipment_type}  
**Model:** {model}  
**Manufacturer:** {manufacturer}  
**Serial Number:** {manufacturer[:2].upper()}-{unique_id:03d}-2024  

## Maintenance Schedule
- **Inspection Interval:** Every {500 + (unique_id % 300)} operating hours
- **Major Service:** Every {2000 + (unique_id % 1000)} operating hours
- **Critical Components Check:** Every {1000 + (unique_id % 500)} operating hours

## Service Procedures

### 1. Pre-Maintenance Safety Checks
- Ensure equipment is completely shut down
- Lock out/tag out all power sources
- Verify zero energy state
- Wear appropriate PPE (safety glasses, gloves, hearing protection)

### 2. Inspection Points
- **Component Condition:** Check for cracks, erosion, or damage
- **Bearing Assembly:** Inspect for excessive wear or noise
- **Lubrication System:** Verify proper oil levels and pressure
- **Cooling System:** Check coolant flow and temperature
- **Vibration Analysis:** Monitor vibration levels during operation

### 3. Lubrication Requirements
- Use only manufacturer-approved lubricants
- Follow specified lubrication intervals
- Monitor oil quality and contamination levels
- Replace filters as scheduled

### 4. Calibration Procedures
- Calibrate sensors every {1000 + (unique_id % 500)} hours
- Verify measurement accuracy
- Check and adjust control system parameters
- Validate safety system functionality

## Quality Control
- Document all measurements and adjustments
- Photograph any damage or unusual conditions
- Verify all safety systems are operational
- Conduct post-maintenance testing

## Notes
- This equipment operates in demanding industrial environments
- Special attention required for critical components
- Consult engineering team for any deviations from standard procedures
- Equipment ID {unique_id} requires specific attention to maintenance schedule
"""
    
    return manual_content

def generate_parts_list(unique_id):
    """Generate a parts list with quantities for the equipment."""
    
    # Select equipment type based on unique_id
    equipment_idx = (unique_id - 1) % len(EQUIPMENT_TYPES)
    part_category = PART_CATEGORIES[equipment_idx]
    
    # Generate parts with realistic quantities based on equipment type
    parts_with_quantities = []
    
    # Equipment-specific parts with realistic quantities
    for i, category in enumerate(part_category):
        part_number = f"{category}-{unique_id:03d}-{i+1:02d}"
        # Vary quantities based on part type and equipment
        if "BEAR" in category:
            quantity = 2 + (unique_id % 3)  # 2-4 bearings
        elif "SEAL" in category:
            quantity = 1 + (unique_id % 2)  # 1-2 seals
        elif "FILTER" in category:
            quantity = 1  # Usually 1 filter
        elif "GREASE" in category:
            quantity = 2 + (unique_id % 4)  # 2-5 grease tubes
        elif "OIL" in category:
            quantity = 1 + (unique_id % 3)  # 1-3 oil containers
        elif "GASKET" in category:
            quantity = 1 + (unique_id % 2)  # 1-2 gaskets
        else:
            quantity = 1 + (unique_id % 3)  # Default 1-3 quantity
        
        parts_with_quantities.append(f"{part_number},{quantity}")
    
    # Add common parts with realistic quantities
    common_parts = [
        (f"GREASE-{unique_id:03d}-01", 2 + (unique_id % 4)),  # 2-5 grease tubes
        (f"OIL-{unique_id:03d}-01", 1 + (unique_id % 3)),     # 1-3 oil containers
        (f"FILTER-{unique_id:03d}-01", 1),                     # 1 filter
        (f"GASKET-{unique_id:03d}-01", 1 + (unique_id % 2))   # 1-2 gaskets
    ]
    
    for part, quantity in common_parts:
        parts_with_quantities.append(f"{part},{quantity}")
    
    return "\n".join(parts_with_quantities)

def main():
    """Generate all service manuals and parts files."""
    
    # Create the service-manuals directory if it doesn't exist
    output_dir = Path("service-manuals")
    output_dir.mkdir(exist_ok=True)
    
    print(f"Generating service manuals and parts files in {output_dir}...")
    
    for unique_id in range(1, 101):
        # Select equipment type
        equipment_idx = (unique_id - 1) % len(EQUIPMENT_TYPES)
        equipment_type, model, manufacturer = EQUIPMENT_TYPES[equipment_idx]
        
        # Generate service manual
        manual_content = generate_service_manual(unique_id, equipment_type, model, manufacturer)
        manual_file = output_dir / f"{unique_id}_manual.md"
        
        with open(manual_file, 'w') as f:
            f.write(manual_content)
        
        # Generate parts list
        parts_content = generate_parts_list(unique_id)
        parts_file = output_dir / f"{unique_id}_parts.txt"
        
        with open(parts_file, 'w') as f:
            f.write(parts_content)
        
        print(f"Generated: {unique_id}_manual.md and {unique_id}_parts.txt")
    
    print(f"\nSuccessfully generated {100} pairs of files!")
    print("Files created:")
    print("- {unique_id}_manual.md - Service manual in markdown format")
    print("- {unique_id}_parts.txt - Parts list for database ingestion")

if __name__ == "__main__":
    main()
