import mlrun
from mlrun.artifacts import get_model
import pandas as pd
import numpy as np
from neuralforecast import NeuralForecast
import cloudpickle


@mlrun.handler()
def predict(
    context: mlrun.MLClientCtx,
    Y_full: pd.DataFrame,
    Y_test_normalized: pd.DataFrame,
    model: mlrun.DataItem,
    unique_id_column: str = 'unique_id',
    ds_column: str = 'ds',
    n_windows: int = 10,
    step_size: int = 1,
):
    """
    Load trained model and perform cross-validation predictions on test data.
    
    Args:
        context: MLRun context
        Y_train_normalized: Normalized training dataframe
        Y_test_normalized: Normalized test dataframe
        model: Trained model from previous training step
        unique_id_column: Name of the unique identifier column (default: 'unique_id')
        ds_column: Name of the time series column (default: 'ds')
        n_windows: Number of cross-validation windows (default: 10)
        step_size: Step size for rolling evaluation (default: 1)
    """
    
    try:
        context.logger.info("Starting prediction process...")
        context.logger.info(f"Full data shape: {Y_full.shape}")
        context.logger.info(f"Test data shape: {Y_test_normalized.shape}")
        
        # Load the trained model
        context.logger.info("Loading trained model...")
        model_file, model_artifact, _ = get_model(model)
        with open(model_file, "rb") as f:
            model = cloudpickle.load(f)

        
        # Perform cross-validation
        context.logger.info(f"Starting cross-validation with {n_windows} windows...")
        cv = model.cross_validation(
            df=Y_full,
            n_windows=n_windows,
            step_size=step_size,
        )
        
        context.logger.info(f"Cross-validation completed. CV results shape: {cv.shape}")
        context.logger.info(f"CV columns: {list(cv.columns)}")
        
        # Keep only rows that are actually in the test set
        context.logger.info("Filtering CV results to test set only...")
        cv_filtered = cv.merge(
            Y_test_normalized[[unique_id_column, ds_column]], 
            on=[unique_id_column, ds_column], 
            how='inner'
        )
        
        context.logger.info(f"Filtered CV results shape: {cv_filtered.shape}")
        
        # Calculate some basic prediction metrics
        if 'y' in cv_filtered.columns and any(col.startswith('NBEATSx') for col in cv_filtered.columns):
            # Find the prediction column (should start with 'NBEATSx')
            pred_col = [col for col in cv_filtered.columns if col.startswith('NBEATSx')][0]
            
            # Calculate basic metrics
            from sklearn.metrics import mean_absolute_error, mean_squared_error
            
            y_true = cv_filtered['y']
            y_pred = cv_filtered[pred_col]
            
            mae = mean_absolute_error(y_true, y_pred)
            mse = mean_squared_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            
            prediction_metrics = {
                "mae": float(mae),
                "mse": float(mse),
                "rmse": float(rmse),
                "n_windows": n_windows,
                "step_size": step_size,
                "predictions_count": len(cv_filtered)
            }
            
            context.logger.info(f"Prediction metrics: {prediction_metrics}")
            
            # Log metrics as artifact
            import json
            context.log_artifact("prediction_metrics", body=json.dumps(prediction_metrics, indent=2), format="json")
        
        # averaged prediction
        predictions = pd.DataFrame(cv_filtered.groupby('unique_id')['NBEATSx'].mean()).reset_index()
       
        # Log the predictions
        context.log_dataset(key="predictions", df=predictions, format="csv", index=False)
        
        context.logger.info("Prediction process completed successfully")
        context.logger.info(f"Final predictions shape: {predictions.shape}")
        context.logger.info(f"Final predictions columns: {list(predictions.columns)}")
        
        return 
        
    except Exception as e:
        context.logger.error(f"Error during prediction: {str(e)}")
        raise e
