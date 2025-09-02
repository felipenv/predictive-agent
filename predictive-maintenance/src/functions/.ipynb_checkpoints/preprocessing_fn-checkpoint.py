import mlrun
import pandas as pd
from sklearn.preprocessing import StandardScaler


def normalize_sensor_columns_globally(train_data: pd.DataFrame, test_data: pd.DataFrame, sensor_columns: list):
    """
    Normalize sensor columns globally using training data statistics.
    Returns the normalized training and test dataframes.
    """
    # Initialize scaler
    scaler = StandardScaler()
    
    # Fit scaler on training data and transform both training and test
    train_normalized = train_data.copy()
    test_normalized = test_data.copy()
    
    # Fit on training data and transform training data
    train_normalized[sensor_columns] = scaler.fit_transform(train_data[sensor_columns])
    
    # Transform test data using the same fitted scaler
    test_normalized[sensor_columns] = scaler.transform(test_data[sensor_columns])
    
    return train_normalized, test_normalized, scaler


@mlrun.handler()
def input_data(
    context: mlrun.MLClientCtx,
    Y_train_df: pd.DataFrame,
    Y_test_df: pd.DataFrame,
):

    # adjust time in test set to align with train
    max_ds = Y_train_df.groupby('unique_id')["ds"].max()
    Y_test_df = Y_test_df.merge(max_ds, on='unique_id', how='left', suffixes=('', '_train_max_date'))
    Y_test_df["ds"] = Y_test_df["ds"] + Y_test_df["ds_train_max_date"]
    Y_test_df = Y_test_df.drop(columns=["ds_train_max_date"])
    
    # Identify sensor columns (those starting with "s_")
    sensor_columns = [col for col in Y_train_df.columns if col.startswith('s_')]
    
    if not sensor_columns:
        context.logger.warning("No sensor columns found starting with 's_'")
        return Y_train_df, Y_test_df
    
    context.logger.info(f"Found {len(sensor_columns)} sensor columns: {sensor_columns}")
    
    # Normalize sensor data globally using training data statistics
    Y_train_normalized, Y_test_normalized, fitted_scaler = normalize_sensor_columns_globally(
        Y_train_df, 
        Y_test_df,
        sensor_columns
    )
    
    # Log the preprocessing results
    context.log_dataset(key="Y_train_normalized", df=Y_train_normalized, format="csv")
    context.log_dataset(key="Y_test_normalized", df=Y_test_normalized, format="csv")
    
    # Log information about the normalization
    context.logger.info(f"Globally normalized {len(sensor_columns)} sensor columns")


    # Concatenate datasets into one for predictive crossvalidation
    Y_full = pd.concat([Y_train_df, Y_test_df], ignore_index=True)

    context.log_dataset(key="full_data_normalized", df=Y_full, format="csv")

    
    return Y_train_normalized, Y_test_normalized
