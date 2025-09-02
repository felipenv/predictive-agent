import os
import sys
from pathlib import Path

import mlrun
from kfp import dsl


@dsl.pipeline()
def pipeline(
    Y_train_df: str,
    Y_test_df: str,
):
    project = mlrun.get_current_project()
    # Add project directory to the system path
    # The project directory is the same as the project name
    project_path = Path(project.metadata.name).resolve()
    sys.path.append(str(project_path))


    preprocessing_fn = project.get_function("preprocessing")

    preprocessing_run = project.run_function(
        function=preprocessing_fn,
        inputs={
            "Y_train_df": Y_train_df,
            "Y_test_df": Y_test_df,
        },
        local=False,
    )

    feature_fn = project.get_function("feature")
    feature_run = project.run_function(
        function=feature_fn,
        inputs={
            "Y_full_data_normalized": preprocessing_run.outputs["full_data_normalized"],
        },
        local=False,
    )
    train_fn = project.get_function("train")
    
    train_run = project.run_function(
        function=train_fn,
        inputs={
            "Y_full_data_features": feature_run.outputs["Y_full_data_features"],
        },
        local=False,
    )