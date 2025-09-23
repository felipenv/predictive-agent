import mlrun
import pandas as pd
import numpy as np
from neuralforecast import NeuralForecast
from neuralforecast.models import NBEATSx
from neuralforecast.losses.pytorch import HuberLoss
import cloudpickle


@mlrun.handler()
def train_model(
    context: mlrun.MLClientCtx,
    Y_full_data_features: pd.DataFrame,
    target_column: str = 'y',
    unique_id_column: str = 'unique_id',
    ds_column: str = 'ds',
    input_size: int = 24,
    horizon: int = 1,
    max_steps: int = 100,
    batch_size: int = 32,
    dropout_prob: float = 0.30,
    mlp_units: list = None,
    n_blocks: list = None,
):
    """
    Train NBEATSx model using NeuralForecast.
    
    Args:
        context: MLRun context
        Y_full_data_features: DataFrame with features including the target and smoothed sensor data
        target_column: Name of the target column (default: 'y')
        unique_id_column: Name of the unique identifier column (default: 'unique_id')
        ds_column: Name of the time series column (default: 'ds')
        input_size: Number of input time steps (default: 24)
        horizon: Forecast horizon (default: 1)
        max_steps: Maximum training steps (default: 100)
        batch_size: Training batch size (default: 32)
        dropout_prob: Dropout probability (default: 0.30)
        mlp_units: MLP units configuration (default: [[64, 128]])
        n_blocks: Number of blocks configuration (default: [1])
    """
    
    try:
        # Set default values for MLP and blocks if not provided
        if mlp_units is None:
            mlp_units = [[64, 128]]
        if n_blocks is None:
            n_blocks = [1]
        
        # Define the specific exogenous variables to use
        futr_exog_list = ['s_2', 's_3', 's_4', 's_7', 's_8', 's_9', 's_11',
                          's_12', 's_13', 's_14', 's_15', 's_17', 's_20', 's_21', 's_21_smoothed']
        
        context.logger.info(f"Starting model training with {len(Y_full_data_features)} samples")
        context.logger.info(f"Target column: {target_column}")
        context.logger.info(f"Unique ID column: {unique_id_column}")
        context.logger.info(f"Time series column: {ds_column}")
        context.logger.info(f"Using {len(futr_exog_list)} exogenous variables: {futr_exog_list}")
        
        # Validate that all specified exogenous variables exist in the data
        missing_exog = [col for col in futr_exog_list if col not in Y_full_data_features.columns]
        if missing_exog:
            raise ValueError(f"Missing exogenous variables in data: {missing_exog}")
        
        
        context.logger.info(f"Data input. Shape: {Y_full_data_features.shape}")
        context.logger.info(f"Columns: {list(Y_full_data_features.columns)}")
        
        
        # Configure NBEATSx model
        lite = NBEATSx(
            h=horizon,
            input_size=input_size,
            loss=HuberLoss(),
            scaler_type='robust',
            stack_types=['identity'],
            dropout_prob_theta=dropout_prob,
            futr_exog_list=futr_exog_list,
            exclude_insample_y=True,
            max_steps=max_steps,
            n_blocks=n_blocks,
            mlp_units=mlp_units,
            batch_size=batch_size,
            valid_batch_size=batch_size,
            drop_last_loader=True,
            dataloader_kwargs={'num_workers': 0},
            early_stop_patience_steps=0,  # Disable early stopping
            accelerator='cpu',
            devices=1,
            precision=32,
            enable_checkpointing=False,
            logger=False,
            num_sanity_val_steps=0,
            limit_val_batches=0.0,  # OK because early stopping is disabled
            enable_progress_bar=True,
        )
        
        context.logger.info("NBEATSx model configured successfully")
        
        # Create NeuralForecast instance
        nf = NeuralForecast(models=[lite], freq=1)
        context.logger.info("NeuralForecast instance created")
        
        # Train the model
        context.logger.info("Starting model training...")
        nf.fit(df=Y_full_data_features)
        context.logger.info("Model training completed successfully")
        
        # Save the trained model
        model_path = "trained_nbeatsx_model"
        nf.save(path=model_path)
        context.logger.info(f"Model saved to {model_path}")
        
        # Get some basic metrics (you can enhance these based on your needs)
        train_metrics = {
            "training_samples": len(Y_full_data_features),
            "unique_series": Y_full_data_features['unique_id'].nunique(),
            "input_size": input_size,
            "horizon": horizon
        }
        
        # Log the model
        context.log_model(
            key="model",  # kfp pipeline key
            db_key="nbeatsx_model",  # mlrun artifact key
            body=cloudpickle.dumps(nf),
            model_file="model.pkl",
            metrics=train_metrics,
            training_set=Y_full_data_features,
            label_column=target_column,
            parameters={
                "model_type": "NBEATSx",
                "input_size": input_size,
                "horizon": horizon,
                "max_steps": max_steps,
                "batch_size": batch_size,
                "dropout_prob": dropout_prob,
                "exogenous_variables": futr_exog_list,
                "stack_types": ['identity'],
                "scaler_type": 'robust',
                "loss": "HuberLoss"
            },
            framework="NeuralForecast",
            algorithm="NBEATSx",
        )
        
        context.logger.info("Model logged successfully to MLRun")
        # Get model summary and log it
        model_summary = {
            "model_type": "NBEATSx",
            "input_size": int(input_size),
            "horizon": int(horizon),
            "max_steps": int(max_steps),
            "batch_size": int(batch_size),
            "dropout_prob": float(dropout_prob),
            "exogenous_variables": futr_exog_list,
            "training_samples": int(len(Y_full_data_features)),
            "unique_series": int(Y_full_data_features['unique_id'].nunique()),
            "date_range": {
                "start": str(Y_full_data_features['ds'].min()),
                "end": str(Y_full_data_features['ds'].max())
            }
        }
        
        context.logger.info(f"Model summary: {model_summary}")
        
        # Log model summary as artifact
        import json
        context.log_artifact("model_summary", body=json.dumps(model_summary, indent=2), format="json")
        
        return model_path
        
    except Exception as e:
        context.logger.error(f"Error during model training: {str(e)}")
        raise e
