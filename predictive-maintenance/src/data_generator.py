import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def generate_synthetic_sensor_data(n_samples: int = 1000, 
                                 start_date: str = "2024-01-01") -> pd.DataFrame:
    """
    Generate synthetic sensor data for predictive maintenance workshop.
    
    Args:
        n_samples: Number of samples to generate
        start_date: Start date for the time series
        
    Returns:
        DataFrame with synthetic sensor data
    """
    logger.info(f"Generating {n_samples} samples of synthetic sensor data")
    
    # Create timestamp series
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    timestamps = [start_dt + timedelta(hours=i) for i in range(n_samples)]
    
    # Generate base sensor readings
    np.random.seed(42)  # For reproducible results
    
    # Air temperature (normal operating range: 20-40°C)
    air_temp = np.random.normal(30, 5, n_samples)
    air_temp = np.clip(air_temp, 15, 45)
    
    # Process temperature (normal operating range: 30-60°C)
    process_temp = air_temp + np.random.normal(10, 3, n_samples)
    process_temp = np.clip(process_temp, 25, 70)
    
    # Rotational speed (normal operating range: 1000-3000 RPM)
    rotational_speed = np.random.normal(2000, 400, n_samples)
    rotational_speed = np.clip(rotational_speed, 800, 3200)
    
    # Torque (normal operating range: 30-80 Nm)
    torque = np.random.normal(55, 10, n_samples)
    torque = np.clip(torque, 20, 90)
    
    # Tool wear (increases over time, 0-1 scale)
    tool_wear = np.linspace(0, 1, n_samples) + np.random.normal(0, 0.05, n_samples)
    tool_wear = np.clip(tool_wear, 0, 1)
    
    # Create DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'air_temperature': air_temp,
        'process_temperature': process_temp,
        'rotational_speed': rotational_speed,
        'torque': torque,
        'tool_wear': tool_wear
    })
    
    # Add machine failure based on tool wear and temperature conditions
    df['machine_failure'] = (
        (df['tool_wear'] > 0.8) | 
        (df['process_temperature'] > 65) |
        (df['air_temperature'] > 40)
    ).astype(int)
    
    # Add equipment_id (simulating multiple machines)
    df['equipment_id'] = np.random.choice(['MACHINE_001', 'MACHINE_002', 'MACHINE_003'], n_samples)
    
    logger.info(f"Generated sensor data with shape: {df.shape}")
    logger.info(f"Failure rate: {df['machine_failure'].mean():.2%}")
    
    return df


def test_data_generator():
    """
    Test function to verify the data generator works correctly.
    """
    print("Testing synthetic data generator...")
    
    # Generate test data
    df = generate_synthetic_sensor_data(n_samples=100)
    
    # Basic checks
    print(f"Data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    print(f"Failure rate: {df['machine_failure'].mean():.2%}")
    print(f"Tool wear range: {df['tool_wear'].min():.3f} - {df['tool_wear'].max():.3f}")
    print(f"Temperature range: {df['air_temperature'].min():.1f}°C - {df['air_temperature'].max():.1f}°C")
    
    # Check for missing values
    missing_values = df.isnull().sum()
    print(f"Missing values:\n{missing_values}")
    
    print("Data generator test completed successfully!")
    return df


if __name__ == "__main__":
    # Run test when script is executed directly
    test_df = test_data_generator()
