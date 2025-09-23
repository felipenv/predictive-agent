import mlrun
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter


def create_smoothed_feature_by_group(data: pd.DataFrame, column_name: str = 's_21', group_column: str = 'unique_id', window_length: int = 15, polyorder: int = 3):
    """
    Create a smoothed version of a sensor column by grouping on unique_id.
    Each group gets its own smoothing treatment.
    
    Args:
        data: Input dataframe
        column_name: Name of the column to smooth (default: 's_21')
        group_column: Column to group by (default: 'unique_id')
        window_length: Length of the filter window (must be odd, default: 15)
        polyorder: Order of the polynomial used to fit the samples (default: 3)
    
    Returns:
        DataFrame with the original column and a new smoothed column
    """
    if column_name not in data.columns:
        raise ValueError(f"Column '{column_name}' not found in the dataframe")
    
    if group_column not in data.columns:
        raise ValueError(f"Group column '{group_column}' not found in the dataframe")
    
    # Make a copy to avoid modifying the original data
    data_with_smoothed = data.copy()
    
    # Ensure window_length is odd (required by savgol_filter)
    if window_length % 2 == 0:
        window_length += 1
    
    # Get unique groups
    unique_groups = data[group_column].unique()
    
    # Create the new smoothed column name
    smoothed_column_name = f"{column_name}_smoothed"
    data_with_smoothed[smoothed_column_name] = np.nan
    
    # Apply smoothing to each group separately
    for group in unique_groups:
        # Get data for this group
        group_mask = data[group_column] == group
        group_data = data[group_mask]
        
        if not group_data.empty:
            # Apply Savitzky-Golay smoothing filter to this group
            try:
                smoothed_values = savgol_filter(
                    group_data[column_name].values, 
                    window_length=min(window_length, len(group_data)), 
                    polyorder=min(polyorder, len(group_data) - 1)
                )
                
                # Update the smoothed column for this group
                data_with_smoothed.loc[group_mask, smoothed_column_name] = smoothed_values
                
            except Exception as e:
                # Fallback to simple moving average if Savitzky-Golay fails
                print(f"Savitzky-Golay filter failed for group {group}: {e}. Falling back to simple moving average.")
                
                # Simple moving average as fallback
                smoothed_values = group_data[column_name].rolling(
                    window=min(window_length, len(group_data)), 
                    center=True, 
                    min_periods=1
                ).mean()
                
                # Update the smoothed column for this group
                data_with_smoothed.loc[group_mask, smoothed_column_name] = smoothed_values
    
    return data_with_smoothed, smoothed_column_name


@mlrun.handler()
def feat_creation(
    context: mlrun.MLClientCtx,
    Y_full_data_normalized: pd.DataFrame,
    sensor_column: str = 's_21',
    group_column: str = 'unique_id',
    window_length: int = 15,
    polyorder: int = 3,
):
    """
    Create smoothed features for both training and test datasets, grouped by unique_id.
    
    Args:
        context: MLRun context
        Y_train_normalized: Normalized training dataframe
        Y_test_normalized: Normalized test dataframe
        sensor_column: Sensor column to smooth (default: 's_21')
        group_column: Column to group by (default: 'unique_id')
        window_length: Length of the smoothing window (default: 15)
        polyorder: Polynomial order for Savitzky-Golay filter (default: 3)
    """
    
    # Validate input
    if sensor_column not in Y_full_data_normalized.columns:
        context.logger.error(f"Column '{sensor_column}' not found in training data")
        return Y_full_data_normalized
    
    if group_column not in Y_full_data_normalized.columns:
        context.logger.error(f"Group column '{group_column}' not found in training data")
        return Y_full_data_normalized
    
    
    context.logger.info(f"Creating smoothed feature for column: {sensor_column}")
    context.logger.info(f"Grouping by: {group_column}")
    context.logger.info(f"Using window_length: {window_length}, polyorder: {polyorder}")
    
    
    # Create smoothed features for training data
    Y_full_data_features, smoothed_col_name = create_smoothed_feature_by_group(
        Y_full_data_normalized, 
        column_name=sensor_column,
        group_column=group_column,
        window_length=window_length,
        polyorder=polyorder
    )
    
    # Log information about the new feature
    context.logger.info(f"Created new feature: {smoothed_col_name}")
    Y_full_data_features = Y_full_data_features.drop(columns=['Unnamed: 0'])
    # Log the datasets
    context.log_dataset(key="Y_full_data_features", df=Y_full_data_features, format="csv")
    

