import os
import mlrun

# Env vars your MinIO-backed MLRun CE needs inside every runtime pod
MINIO_KEYS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "S3_ENDPOINT_URL",
    "S3_USE_HTTPS",
    "S3_VERIFY_SSL",
    "AWS_DEFAULT_REGION",
    "S3_NON_ANONYMOUS",
    "S3_USE_PATH_STYLE",  # important for MinIO path-style addressing
]


def _inject_minio_env(fn: mlrun.runtimes.KubejobRuntime) -> None:
    """
    Inject MinIO/S3 environment variables into the function's runtime pod.
    Uses plain env vars (compatible with MLRun 1.9.x KubejobRuntime).
    """
    # Ensure path-style on MinIO (safe even if already set)
    os.environ.setdefault("S3_USE_PATH_STYLE", "1")

    for key in MINIO_KEYS:
        value = os.getenv(key)
        if value is not None:
            fn.set_env(key, value)


def setup(project: mlrun.projects.MlrunProject) -> mlrun.projects.MlrunProject:
    source = project.get_param("source")
    secrets_file = project.get_param("secrets_file")
    default_image = project.get_param("default_image")

    # Load secrets if provided
    if secrets_file and os.path.exists(secrets_file):
        project.set_secrets(file_path=secrets_file)
        mlrun.set_env_from_file(secrets_file)

    # Set project source if specified
    if source:
        print(f"Project Source: {source}")
        project.set_source(source, pull_at_runtime=True)
        if ".zip" in source:
            print(f"Exporting project as zip archive to {source}...")
            project.export(source)

    # Use manually built Docker image from Docker Hub
    if default_image:
        project.set_default_image(default_image)

    # Register project functions for predictive maintenance
    functions = {
        "preprocessing": (
            "src/functions/preprocessing_fn.py",
            "input_data",
            True,
        ),
        "feature": (
            "src/functions/feature_fn.py",
            "feat_creation",
            True,
        ),
        "train": (
            "src/functions/train_fn.py",
            "train_model",
            True,
        ),
        "prediction": (
            "src/functions/prediction_fn.py",
            "predict",
            True,
        ),
    }

    for name, (func_path, handler, with_repo) in functions.items():
        fn = project.set_function(
            name=name,
            func=func_path,
            handler=handler,
            kind="job",
            with_repo=with_repo,
        )
        fn.spec.image_pull_policy = "IfNotPresent"
        _inject_minio_env(fn)

    # Set up workflows
    project.set_workflow(
        name="training-workflow",
        workflow_path="src/workflows/training_workflow.py",
        image=default_image,
    )
    # project.set_workflow(
    #     name="inference-workflow",
    #     workflow_path="src/workflows/inference_workflow.py",
    #     image=default_image,
    # )


    project.save()
    return project
