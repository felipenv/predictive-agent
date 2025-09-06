import datetime as dt
import json
import os
from typing import Annotated, Tuple

import mlrun
import pandas as pd
import plotly.graph_objects as go
import requests
from fastmcp import FastMCP
from mlrun.artifacts import Artifact
from pydantic import Field

# Set MinIO environment variables for S3 access
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")
os.environ.setdefault("S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("S3_USE_HTTPS", "False")
os.environ.setdefault("S3_VERIFY_SSL", "False")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("S3_NON_ANONYMOUS", "True")
os.environ.setdefault("S3_USE_PATH_STYLE", "True")

project = mlrun.get_or_create_project(
    "predictive-maintenance",
    parameters={
        "source": "s3://mlrun/predictive-maintenance.zip",
        "default_image": "felipenv/mlrun-predictive:1.9",
    },
)

# Set MinIO credentials as project secrets for workflow access
project.set_secrets(
    secrets={
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "S3_ENDPOINT_URL": "http://minio:9000",
        "S3_USE_HTTPS": "False",
        "S3_VERIFY_SSL": "False",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_NON_ANONYMOUS": "True",
        "S3_USE_PATH_STYLE": "True"
    }
)

db = mlrun.get_run_db()

mcp = FastMCP("MLRun Predictive Maintenance MCP")

BASE_URL = "http://mlrun-ui.mlrun.svc.cluster.local:80/mlrun"


@mcp.prompt
def project_report() -> str:
    return f"""
Generate a project report with the following structure and content.
Use human-readable names for artifacts, models, and workflows.
Structure the report as follows:

---

MLRun Project Report: {project.metadata.name}

---

Models  
For each model in the project:
- List evaluation metrics (e.g., accuracy, F1, AUC, etc.) in a nicely structured bulleted list (comparing training vs test metrics where applicable)
- Include the feature set used to train the model
- Add a link to the model artifact
- Add a link to the input dataset used to train or validate the model

---

Latest Workflow Runs  
Show the most recent workflow runs:
- Display run names, timestamps, and statuses (success, error, running, etc.)
- Clearly indicate if any workflows failed or encountered errors

""".strip()


def _format_timedelta(td: dt.timedelta) -> str:
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)  # 86400 = 24*60*60
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:  # Show 0s if all other parts are zero
        parts.append(f"{seconds}s")

    return " ".join(parts)


def _get_artifact_key_tag(artifact: Artifact) -> Tuple[str, str, bool]:
    # Use UID as tag if no tag is set
    if artifact.metadata.tag:
        tag = artifact.metadata.tag
        has_tag = True
    else:
        tag = artifact.uid
        has_tag = False
    return artifact.spec.db_key, tag, has_tag


@mcp.tool
def list_datasets() -> list[str]:
    """List all datasets with tags in the project."""
    datasets = project.list_artifacts(kind="dataset")
    return list(
        {
            f'{d["spec"]["db_key"]}:{d["metadata"]["tag"]}'
            for d in datasets
            if d["metadata"]["tag"]
        }
    )


@mcp.tool
def list_all_artifacts() -> dict[str, list[str]]:
    """List all artifacts in the project grouped by type."""
    all_artifacts = project.list_artifacts().to_objects()

    # Group artifacts by kind
    artifacts_by_kind = {}
    for artifact in all_artifacts:
        kind = artifact.kind
        key, tag, _ = _get_artifact_key_tag(artifact)

        if kind not in artifacts_by_kind:
            artifacts_by_kind[kind] = []

        artifacts_by_kind[kind].append(f"{key}:{tag}")

    # Sort each list and remove duplicates
    for kind in artifacts_by_kind:
        artifacts_by_kind[kind] = sorted(list(set(artifacts_by_kind[kind])))

    return artifacts_by_kind


@mcp.tool
def list_models() -> list[str]:
    """List all models with tags in the project."""
    return list(
        {
            f"{m.spec.db_key}:{m.metadata.tag}"
            for m in project.list_models()
            if m.metadata.tag
        }
    )


@mcp.tool
def describe_model(
    key: Annotated[str, Field(description="Name of the model (ex: 'my_model')")],
    tag: Annotated[str, Field(description="Tag of the model (ex: 'latest')")],
) -> str:
    """Get a description of a specific model artifact including its metadata, metrics and data sources."""
    try:
        model_artifact = project.get_artifact(key=key, tag=tag)
        report = []
        report.append(f"Model: {key}:{tag}")
        
        # Handle labels safely
        if hasattr(model_artifact, 'labels') and model_artifact.labels:
            workflow_id = model_artifact.labels.get('workflow-id', 'N/A')
            report.append(f"Workflow ID: {workflow_id}")
        else:
            report.append("Workflow ID: N/A")
        
        # Handle spec safely
        if hasattr(model_artifact, 'spec') and model_artifact.spec:
            framework = getattr(model_artifact.spec, 'framework', 'N/A')
            algorithm = getattr(model_artifact.spec, 'algorithm', 'N/A')
            report.append(f"Framework: {framework}")
            report.append(f"Algorithm: {algorithm}")
            
            # Handle data sources safely
            if hasattr(model_artifact.spec, 'sources') and model_artifact.spec.sources:
                report.append("Data Sources:")
                for source in model_artifact.spec.sources:
                    try:
                        if isinstance(source, dict) and 'path' in source and 'name' in source:
                            artifact = mlrun.get_dataitem(source["path"]).meta
                            source_key, source_tag, _ = _get_artifact_key_tag(artifact)
                            report.append(f"{source['name']}: {source_key}:{source_tag}")
                        else:
                            report.append(f"Source: {str(source)}")
                    except Exception as e:
                        report.append(f"Source: {str(source)} (Error: {str(e)})")
            else:
                report.append("Data Sources: None")
        else:
            report.append("Framework: N/A")
            report.append("Algorithm: N/A")
            report.append("Data Sources: N/A")
        
        # Handle parameters safely
        if hasattr(model_artifact, 'parameters') and model_artifact.parameters:
            repr_val = model_artifact.parameters.get('repr', 'N/A')
            report.append(f"Repr: {repr_val}")
        else:
            report.append("Repr: N/A")
        
        # Handle metrics safely
        if hasattr(model_artifact, 'metrics') and model_artifact.metrics:
            report.append(f"Metrics: {model_artifact.metrics}")
        else:
            report.append("Metrics: None")
        
        # Add basic artifact info
        if hasattr(model_artifact, 'uri'):
            report.append(f"URI: {model_artifact.uri}")
        if hasattr(model_artifact, 'created'):
            report.append(f"Created: {model_artifact.created}")
        if hasattr(model_artifact, 'updated'):
            report.append(f"Updated: {model_artifact.updated}")
        
        return "\n".join(report)
        
    except Exception as e:
        return f"Error describing model {key}:{tag}: {str(e)}"


@mcp.tool
def get_artifact_uri(
    key: Annotated[str, Field(description="Name of the artifact (ex: 'my_dataset')")],
    tag: Annotated[str, Field(description="Tag of the artifact (ex: 'latest')")],
) -> str:
    """
    Get the URI of a specific artifact like model, dataset, etc.
    Required for operations like retraining a model.
    """
    try:
        return project.get_artifact(key=key, tag=tag).uri
    except mlrun.errors.MLRunNotFoundError:
        # Use the UID for untagged artifacts
        return project.get_artifact(key=key, uid=tag).uri


@mcp.tool
def get_artifact_dashboard_link(
    store_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the artifact to get the dashboard link for."
        ),
    ],
) -> str:
    """
    Get the dashboard link for a specific dataset, model, or artifact. Use get_artifact_uri to retrieve the correct URI.
    """
    if not store_uri.startswith("store://"):
        raise ValueError(
            "Store URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )
    try:
        artifact = mlrun.get_dataitem(store_uri).meta
    except mlrun.errors.MLRunNotFoundError:
        raise ValueError(
            f"Artifact not found for URI: {store_uri}. Did you use get_artifact_uri?"
        )
    if artifact.kind == "dataset":
        kind = "datasets"
    elif artifact.kind == "model":
        kind = "models/models"
    elif artifact.kind == "artifact":
        kind = "files"
    else:
        raise ValueError(
            "Unsupported artifact kind. Only 'dataset', 'model', and 'artifact' are supported."
        )
    key, tag, has_tag = _get_artifact_key_tag(artifact)
    version = f":{tag}" if has_tag else "all-versions/"
    return f"{BASE_URL}/projects/{project.metadata.name}/{kind}/{key}/{version}@{artifact.uid}/overview"


@mcp.tool
def download_artifact(
    key: Annotated[str, Field(description="Name of the artifact to download")],
    tag: Annotated[str, Field(description="Tag of the artifact (ex: 'latest')")],
    local_path: Annotated[
        str, Field(description="Local path where to save the artifact")
    ] = None,
) -> str:
    """
    Download an MLRun artifact to a local file.
    Returns the local file path where the artifact was saved.
    """
    try:
        # Get the artifact
        artifact = project.get_artifact(key=key, tag=tag)

        # Get the dataitem and download content
        dataitem = mlrun.get_dataitem(artifact.uri)
        content = dataitem.get()

        # Determine local path if not provided
        if local_path is None:
            # Create a temporary file with appropriate extension
            file_extension = f".{artifact.format}" if artifact.format else ""
            if not file_extension and "plot" in key.lower():
                file_extension = ".html"  # Most plots are HTML
            local_path = (
                f"/tmp/{key.replace('/', '_').replace('-', '_')}{file_extension}"
            )

        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Save the content
        with open(local_path, "wb") as f:
            f.write(content)

        file_size = os.path.getsize(local_path)
        return f"Artifact '{key}:{tag}' downloaded successfully to: {local_path} (Size: {file_size} bytes)"

    except Exception as e:
        return f"Error downloading artifact '{key}:{tag}': {str(e)}"


def _get_workflow_dashboard_link(workflow_id: str) -> str:
    """
    Get the dashboard link for a specific workflow using its ID.
    Required because you cannot directly call mcp tools in other places
    """
    return f"{BASE_URL}/projects/{project.metadata.name}/jobs/monitor-workflows/workflow/{workflow_id}"


@mcp.tool
def get_workflow_dashboard_link(
    workflow_id: Annotated[
        str,
        Field(description="ID of the workflow to get the dashboard link for."),
    ],
) -> str:
    """
    Get the dashboard link for a specific workflow using its ID.
    """
    return _get_workflow_dashboard_link(workflow_id)


@mcp.tool
def list_workflow_names() -> list[str]:
    """List the name of all workflows/pipelines in the project."""
    return [f"{project.metadata.name}-{w['name']}" for w in project.workflows]


@mcp.tool
def list_workflow_runs(
    workflow_name: Annotated[str, Field(description="Name of the workflow")],
    days_ago: Annotated[
        int,
        Field(
            description="Number of days in the past to query. Leave blank for no date filter."
        ),
    ] = -1,
    limit: Annotated[
        int,
        Field(
            description="Maximum number of workflow runs to return. Leave blank for no limit."
        ),
    ] = -1,
) -> list[dict]:
    """
    List previous workflow runs, date, and status for a specific workflow name.
    Optionally filter by days ago and limit the number of runs returned.
    """
    predicates = [{"key": "name", "op": 9, "string_value": workflow_name}]
    if days_ago > 0:
        start_time = (dt.datetime.now() - dt.timedelta(days=days_ago)).isoformat(
            timespec="milliseconds"
        ) + "Z"
        predicates.append({"key": "created_at", "op": 5, "timestamp_value": start_time})

    pipelines = db.list_pipelines(
        project=project.metadata.name,
        filter_=[json.dumps({"predicates": predicates})],
    )
    pipelines_df = pd.DataFrame(pipelines.runs)
    if pipelines_df.empty:
        return []
    pipelines_df = pipelines_df.sort_values(by=["created_at"], ascending=False)[
        ["id", "created_at", "status"]
    ]
    if limit > 0:
        pipelines_df = pipelines_df.head(limit)

    return pipelines_df.to_dict(orient="records")



@mcp.tool
def diagnose_workflow(
    workflow_id: Annotated[str, Field(description="ID of the workflow to diagnose")],
) -> list[dict]:
    """
    Diagnose a workflow by checking the status of each step and calculating elapsed time.
    Returns a list of dictionaries with step name, state, elapsed time, and error if any.
    If necessary, suggest potential fixes and/or fetch the dashboard link for the user.
    """
    runs_df = project.list_runs(labels=f"workflow={workflow_id}").to_df()
    runs_df = runs_df.sort_values(by="start")
    report = []
    for run in runs_df.itertuples():
        elapsed_time = (run.end - run.start).to_pytimedelta()
        step_report = {
            "step": run.name,
            "state": run.state,
            "time_elapsed": _format_timedelta(elapsed_time),
        }
        if run.state == "error":
            step_report["error"] = run.error
        report.append(step_report)
    return report


@mcp.tool
def run_inference_workflow(
    y_train_df_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the training dataset for preprocessing."
        ),
    ],
    y_test_df_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the test dataset for preprocessing."
        ),
    ],
    model_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the trained model to use for inference."
        ),
    ],
) -> str:
    """
    Run inference workflow using the specified training data, test data, and trained model.
    This workflow will:
    1. Preprocess the data (normalize sensor columns)
    2. Create features (smooth s_21 sensor)
    3. Make predictions using the trained model
    """

    if not y_train_df_uri.startswith("store://"):
        raise ValueError(
            "Training dataset URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )
    if not y_test_df_uri.startswith("store://"):
        raise ValueError(
            "Test dataset URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )
    if not model_uri.startswith("store://"):
        raise ValueError(
            "Model URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )

    inference_run_id = project.run(
        name="inference-workflow",
        arguments={
            "y_train_df": y_train_df_uri,
            "y_test_df": y_test_df_uri,
            "model": model_uri,
        },
        engine="remote",
        watch=False,
        dirty=True,
    )
    url = _get_workflow_dashboard_link(inference_run_id)
    output = f"Inference workflow started successfully! View it in the browser at {url}."
    return output


@mcp.tool
def get_predictions_top_k(
    key: Annotated[str, Field(description="Name of the predictions dataset (ex: 'prediction_predictions')")],
    tag: Annotated[str, Field(description="Tag of the predictions dataset (ex: 'latest')")],
    k: Annotated[int, Field(description="Number of top predictions to display (e.g., 10, 20, 50)")],
) -> str:
    """
    Get the predictions dataset, sort by NBEATSx (highest first), and display the top K results in markdown format.
    """
    try:
        # Get the predictions dataset
        predictions_artifact = project.get_artifact(key=key, tag=tag)
        predictions_df = predictions_artifact.to_dataitem().as_df()
        
        # Sort by NBEATSx predictions (descending order to see highest predictions first)
        predictions_df_sorted = predictions_df.sort_values('NBEATSx', ascending=False).reset_index(drop=True)
        
        # Get top K results
        top_k_df = predictions_df_sorted.head(k)
        
        # Create markdown table
        markdown_output = f"## Top {k} Predictions (Sorted by NBEATSx - Highest First)\n\n"
        markdown_output += f"**Dataset:** {key}:{tag}\n"
        markdown_output += f"**Total Records:** {len(predictions_df)}\n"
        markdown_output += f"**Columns:** {', '.join(predictions_df.columns.tolist())}\n\n"
        
        # Create markdown table header
        markdown_output += "| " + " | ".join(top_k_df.columns.tolist()) + " |\n"
        markdown_output += "| " + " | ".join(["---"] * len(top_k_df.columns)) + " |\n"
        
        # Add data rows
        for _, row in top_k_df.iterrows():
            markdown_output += "| " + " | ".join([str(val) for val in row.values]) + " |\n"
        
        # Add summary statistics if 'y' column exists (actual values)
        if 'y' in predictions_df.columns and 'NBEATSx' in predictions_df.columns:
            markdown_output += f"\n## Summary Statistics\n\n"
            markdown_output += f"- **Mean Actual (y):** {predictions_df['y'].mean():.2f}\n"
            markdown_output += f"- **Mean Prediction:** {predictions_df['NBEATSx'].mean():.2f}\n"
            markdown_output += f"- **Mean Absolute Error:** {abs(predictions_df['y'] - predictions_df['NBEATSx']).mean():.2f}\n"
            markdown_output += f"- **Root Mean Square Error:** {((predictions_df['y'] - predictions_df['NBEATSx'])**2).mean()**0.5:.2f}\n"
        
        return markdown_output
        
    except Exception as e:
        return f"Error retrieving predictions dataset: {str(e)}"


@mcp.tool
def get_predictions_lowest_k(
    key: Annotated[str, Field(description="Name of the predictions dataset (ex: 'prediction_predictions')")],
    tag: Annotated[str, Field(description="Tag of the predictions dataset (ex: 'latest')")],
    k: Annotated[int, Field(description="Number of lowest predictions to display (e.g., 10, 20, 50)")],
) -> str:
    """
    Get the predictions dataset, sort by NBEATSx (lowest first), and display the bottom K results in markdown format.
    """
    try:
        # Get the predictions dataset
        predictions_artifact = project.get_artifact(key=key, tag=tag)
        predictions_df = predictions_artifact.to_dataitem().as_df()
        
        # Sort by NBEATSx predictions (ascending order to see lowest predictions first)
        predictions_df_sorted = predictions_df.sort_values('NBEATSx', ascending=True).reset_index(drop=True)
        
        # Get bottom K results (lowest predictions)
        lowest_k_df = predictions_df_sorted.head(k)
        
        # Create markdown table
        markdown_output = f"## Bottom {k} Predictions (Sorted by NBEATSx - Lowest First)\n\n"
        markdown_output += f"**Dataset:** {key}:{tag}\n"
        markdown_output += f"**Total Records:** {len(predictions_df)}\n"
        markdown_output += f"**Columns:** {', '.join(predictions_df.columns.tolist())}\n\n"
        
        # Create markdown table header
        markdown_output += "| " + " | ".join(lowest_k_df.columns.tolist()) + " |\n"
        markdown_output += "| " + " | ".join(["---"] * len(lowest_k_df.columns)) + " |\n"
        
        # Add data rows
        for _, row in lowest_k_df.iterrows():
            markdown_output += "| " + " | ".join([str(val) for val in row.values]) + " |\n"
        
        # Add summary statistics if 'y' column exists (actual values)
        if 'y' in predictions_df.columns and 'NBEATSx' in predictions_df.columns:
            markdown_output += f"\n## Summary Statistics (Lowest Predictions)\n\n"
            markdown_output += f"- **Lowest Prediction:** {predictions_df['NBEATSx'].min():.2f}\n"
            markdown_output += f"- **Highest Prediction:** {predictions_df['NBEATSx'].max():.2f}\n"
            markdown_output += f"- **Prediction Range:** {predictions_df['NBEATSx'].max() - predictions_df['NBEATSx'].min():.2f}\n"
            markdown_output += f"- **Standard Deviation:** {predictions_df['NBEATSx'].std():.2f}\n"
        
        return markdown_output
        
    except Exception as e:
        return f"Error retrieving predictions dataset: {str(e)}"


@mcp.tool
def run_training_workflow(
    y_train_df_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the training dataset. Use get_artifact_uri to retrieve the correct URI."
        ),
    ],
    y_test_df_uri: Annotated[
        str,
        Field(
            description="Artifact URI (store://...) of the test dataset. Use get_artifact_uri to retrieve the correct URI."
        ),
    ],
) -> str:
    """
    Run training workflow using the specified training and test datasets.
    This workflow will:
    1. Preprocess the data (normalize sensor columns)
    2. Create features (smooth s_21 sensor)
    3. Train an NBEATSx model
    """

    if not y_train_df_uri.startswith("store://"):
        raise ValueError(
            "Training dataset URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )
    if not y_test_df_uri.startswith("store://"):
        raise ValueError(
            "Test dataset URI must start with 'store://'. Use get_artifact_uri to retrieve the correct URI."
        )

    training_run_id = project.run(
        name="training-workflow",
        arguments={
            "y_train_df": y_train_df_uri,
            "y_test_df": y_test_df_uri,
        },
        engine="remote",
        watch=False,
        dirty=True,
    )
    url = _get_workflow_dashboard_link(training_run_id)
    output = f"Training workflow started successfully! View it in the browser at {url}."
    return output


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000, path="/")
