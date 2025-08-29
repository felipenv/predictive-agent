import os
import pickle
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import mlrun
from sklearn.metrics import classification_report, confusion_matrix, mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_sensor_data(file_path: str) -> pd.DataFrame:
    """
    Load sensor data from CSV file.
    
    Args:
        file_path: Path to the sensor data CSV file
        
    Returns:
        DataFrame with sensor data
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded sensor data with shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error loading sensor data: {e}")
        raise


def create_time_features(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Create time-based features from timestamp column.
    
    Args:
        df: Input DataFrame
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with additional time features
    """
    df = df.copy()
    
    if timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df['hour'] = df[timestamp_col].dt.hour
        df['day_of_week'] = df[timestamp_col].dt.dayofweek
        df['month'] = df[timestamp_col].dt.month
        df['day_of_year'] = df[timestamp_col].dt.dayofyear
        
    return df


def create_lag_features(df: pd.DataFrame, columns: List[str], lags: List[int]) -> pd.DataFrame:
    """
    Create lag features for time series analysis.
    
    Args:
        df: Input DataFrame
        columns: List of columns to create lags for
        lags: List of lag periods
        
    Returns:
        DataFrame with lag features
    """
    df = df.copy()
    
    for col in columns:
        for lag in lags:
            df[f'{col}_lag_{lag}'] = df[col].shift(lag)
            
    return df


def create_rolling_features(df: pd.DataFrame, columns: List[str], windows: List[int]) -> pd.DataFrame:
    """
    Create rolling window features.
    
    Args:
        df: Input DataFrame
        columns: List of columns to create rolling features for
        windows: List of window sizes
        
    Returns:
        DataFrame with rolling features
    """
    df = df.copy()
    
    for col in columns:
        for window in windows:
            df[f'{col}_rolling_mean_{window}'] = df[col].rolling(window=window).mean()
            df[f'{col}_rolling_std_{window}'] = df[col].rolling(window=window).std()
            df[f'{col}_rolling_min_{window}'] = df[col].rolling(window=window).min()
            df[f'{col}_rolling_max_{window}'] = df[col].rolling(window=window).max()
            
    return df


def evaluate_classification_model(y_true: np.ndarray, y_pred: np.ndarray, 
                                context: mlrun.MLClientCtx, model_name: str = "model") -> Dict[str, float]:
    """
    Evaluate classification model and log metrics.
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        context: MLRun context
        model_name: Name of the model for logging
        
    Returns:
        Dictionary with evaluation metrics
    """
    # Calculate metrics
    report = classification_report(y_true, y_pred, output_dict=True)
    
    # Log metrics
    context.log_result(f"{model_name}_accuracy", report['accuracy'])
    context.log_result(f"{model_name}_precision", report['weighted avg']['precision'])
    context.log_result(f"{model_name}_recall", report['weighted avg']['recall'])
    context.log_result(f"{model_name}_f1_score", report['weighted avg']['f1-score'])
    
    # Create confusion matrix plot
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.title(f'Confusion Matrix - {model_name}')
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    
    # Log confusion matrix
    context.log_artifact(f"{model_name}_confusion_matrix", plt.gcf(), format="png")
    plt.close()
    
    return {
        'accuracy': report['accuracy'],
        'precision': report['weighted avg']['precision'],
        'recall': report['weighted avg']['recall'],
        'f1_score': report['weighted avg']['f1-score']
    }


def evaluate_regression_model(y_true: np.ndarray, y_pred: np.ndarray,
                            context: mlrun.MLClientCtx, model_name: str = "model") -> Dict[str, float]:
    """
    Evaluate regression model and log metrics.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        context: MLRun context
        model_name: Name of the model for logging
        
    Returns:
        Dictionary with evaluation metrics
    """
    # Calculate metrics
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    
    # Log metrics
    context.log_result(f"{model_name}_mae", mae)
    context.log_result(f"{model_name}_mse", mse)
    context.log_result(f"{model_name}_rmse", rmse)
    
    # Create scatter plot
    plt.figure(figsize=(8, 6))
    plt.scatter(y_true, y_pred, alpha=0.5)
    plt.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], 'r--', lw=2)
    plt.xlabel('True Values')
    plt.ylabel('Predicted Values')
    plt.title(f'Prediction vs True Values - {model_name}')
    
    # Log scatter plot
    context.log_artifact(f"{model_name}_scatter_plot", plt.gcf(), format="png")
    plt.close()
    
    return {
        'mae': mae,
        'mse': mse,
        'rmse': rmse
    }


def save_model(model: Any, model_path: str, context: mlrun.MLClientCtx, model_name: str = "model"):
    """
    Save model and log it to MLRun.
    
    Args:
        model: Trained model object
        model_path: Path to save the model
        context: MLRun context
        model_name: Name of the model
    """
    try:
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        context.log_model(model_name, model_file=model_path)
        logger.info(f"Model saved successfully to {model_path}")
        
    except Exception as e:
        logger.error(f"Error saving model: {e}")
        raise


def load_model(model_path: str) -> Any:
    """
    Load a saved model.
    
    Args:
        model_path: Path to the saved model
        
    Returns:
        Loaded model object
    """
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"Model loaded successfully from {model_path}")
        return model
        
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        raise


def calculate_remaining_useful_life(df: pd.DataFrame, failure_threshold: float = 0.8) -> pd.DataFrame:
    """
    Calculate remaining useful life based on tool wear.
    
    Args:
        df: DataFrame with tool wear data
        failure_threshold: Threshold for failure prediction
        
    Returns:
        DataFrame with RUL column
    """
    df = df.copy()
    
    # Calculate RUL based on tool wear
    df['rul'] = failure_threshold - df['tool_wear']
    df['rul'] = df['rul'].clip(lower=0)
    
    return df


def generate_maintenance_schedule(predictions: pd.DataFrame, 
                                maintenance_threshold: float = 0.6) -> pd.DataFrame:
    """
    Generate maintenance schedule based on failure predictions.
    
    Args:
        predictions: DataFrame with failure predictions
        maintenance_threshold: Threshold for maintenance scheduling
        
    Returns:
        DataFrame with maintenance schedule
    """
    schedule = predictions.copy()
    
    # Flag when maintenance is needed
    schedule['maintenance_needed'] = schedule['failure_probability'] > maintenance_threshold
    
    # Calculate days until maintenance
    schedule['days_until_maintenance'] = np.where(
        schedule['maintenance_needed'],
        schedule['rul'].clip(upper=30),  # Max 30 days ahead
        999  # No maintenance needed
    )
    
    return schedule
