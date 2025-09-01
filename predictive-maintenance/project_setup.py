import os
import mlrun


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

    # # Register project functions for predictive maintenance
    # functions = {
    #     "data-preprocessing": (
    #         "src/functions/data_preprocessing_fn.py",
    #         "preprocess_sensor_data",
    #         True,
    #     ),
    #     "feature-engineering": (
    #         "src/functions/feature_engineering_fn.py",
    #         "engineer_features",
    #         True,
    #     ),
    #     "model-training": (
    #         "src/functions/model_training_fn.py",
    #         "train_models",
    #         True,
    #     ),
    #     "failure-prediction": (
    #         "src/functions/failure_prediction_fn.py",
    #         "predict_failures",
    #         True,
    #     ),
    #     "maintenance-recommendation": (
    #         "src/functions/maintenance_recommendation_fn.py",
    #         "recommend_maintenance",
    #         True,
    #     ),
    #     "parts-inventory": (
    #         "src/functions/parts_inventory_fn.py",
    #         "check_parts_availability",
    #         True,
    #     ),
    #     "rag-manual-search": (
    #         "src/functions/rag_manual_search_fn.py",
    #         "search_maintenance_manuals",
    #         True,
    #     ),
    #     "maintenance-scheduler": (
    #         "src/functions/maintenance_scheduler_fn.py",
    #         "schedule_maintenance",
    #         True,
    #     ),
    #     "impact-analysis": (
    #         "src/functions/impact_analysis_fn.py",
    #         "analyze_maintenance_impact",
    #         True,
    #     ),
    # }

    # for name, (func_path, handler, with_repo) in functions.items():
    #     fn = project.set_function(
    #         name=name,
    #         func=func_path,
    #         handler=handler,
    #         kind="job",
    #         with_repo=with_repo,
    #     )
    #     fn.spec.image_pull_policy = "IfNotPresent"

    # # Set up workflows
    # project.set_workflow(
    #     name="training-workflow",
    #     workflow_path="src/workflows/training_workflow.py",
    #     image=default_image,
    # )
    # project.set_workflow(
    #     name="inference-workflow",
    #     workflow_path="src/workflows/inference_workflow.py",
    #     image=default_image,
    # )
    # project.set_workflow(
    #     name="maintenance-workflow",
    #     workflow_path="src/workflows/maintenance_workflow.py",
    #     image=default_image,
    # )

    project.save()
    return project
