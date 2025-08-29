from typing import Union

import yaml
from pydantic import BaseModel


class PreprocessingConfig(BaseModel):
    datetime_col: str
    unified_timestamp: str
    round_timestamps_frequency: str
    outliers_rule: str
    interpolate_kwargs: dict
    resample_errors: str


class FeatureFactoryConfig(BaseModel):
    feature_config: dict
    target_column: str


class FeatureReportConfig(BaseModel):
    target_column: str
    model_features: list[str]
    datetime_column: str
    report_title: str


class ModelingConfig(BaseModel):
    model_name: str
    target_column: str
    model_features: list[str]
    split_method: str
    splitting_parameters: dict
    model_factory_type: str
    model_init_config: dict
    model_tuner_type: str
    tuner_config: dict
    hyperparameters: Union[dict, None]
    cv_strategy_config: Union[dict, None]


class ModelingReportConfig(BaseModel):
    timestamp_column: str
    report_title: str


class OptimizationConfig(BaseModel):
    controlled_parameters: list[dict]
    problem_spec: dict
    problem_factory_class: str
    solver: dict
    stopper: dict
    n_jobs: int
    backend: str
    objective_units: str
    target_column: str
    report_title: str


class CalculateImpactConfig(BaseModel):
    counterfactual_type: str
    target_column: str
    datetime_column: str
    baseline_column: str
    after_implementation_column: str
    original_granularity: str
    group_characteristics: dict
    default_group: str
    agg_granularity: str
    agg_granularity_function: str
    agg_granularity_method: str
    baseline_alternative_hypothesis: str
    uplifts_alternative_hypothesis: str
    annualize_impact: bool


class SampleLiveDataConfig(BaseModel):
    single_sample: bool


class LivePredictionsConfig(BaseModel):
    anomaly_parameters: dict
    tags_to_monitor: list[str]
    datetime_column: str
    target_column: str
    error_metric: str
    error_multiplier: float
    counterfactual_type: str
    n_jobs: int


class UpdateDBConfig(BaseModel):
    datetime_column: str
    iso_format: str
    plant_name: str
    target_column: str
    controlled_parameters: list[dict]
    plant_info: list[dict]
    tags_meta: list[dict]
    targets_meta: list[dict]


class Config(BaseModel):
    preprocessing: PreprocessingConfig
    feature_factory: FeatureFactoryConfig
    feature_report: FeatureReportConfig
    modeling: ModelingConfig
    baseline_modeling: ModelingConfig
    modeling_report: ModelingReportConfig
    optimization: OptimizationConfig
    calculate_impact: CalculateImpactConfig
    sample_live_data: SampleLiveDataConfig
    live_predictions: LivePredictionsConfig
    update_db: UpdateDBConfig


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as file:
        c = yaml.safe_load(file)
    return Config(**c)
