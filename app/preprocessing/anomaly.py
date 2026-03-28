import ast
from pathlib import Path

import pandas as pd
from joblib import dump, load
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import OneClassSVM

from app.config import (
    ANOMALIES_PATH,
    ONE_CLASS_SVM_MODEL_PATH,
)

TEXT_COLUMN = "parsed_logline"

CATEGORICAL_COLUMNS = [
    "process_name",
    "process_subtype",
    "host",
    "remote_host",
    "user_value",
    "tty",
    "month",
]

NUMERIC_COLUMNS = [
    "day",
    "process_pid",
    "parameter_count",
    "logline_length",
    "parsed_logline_length",
]


def detect_anomalies(
    log_features: pd.DataFrame,
    train_features: pd.DataFrame | None = None,
    algo_name: str = "one_class_svm",
    output_path: Path = ANOMALIES_PATH,
) -> pd.DataFrame:

    required_columns = {"row_no", "parsed_logline"}
    missing_columns = required_columns.difference(log_features.columns)
    if missing_columns:
        raise KeyError(f"Missing columns: {missing_columns}")

    if train_features is None:
        train_features = log_features

    results = _detect_anomalies_ocsvm(log_features, train_features)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(output_path, index=False)
    return results


def _detect_anomalies_ocsvm(
    log_features: pd.DataFrame,
    train_features: pd.DataFrame,
) -> pd.DataFrame:

    inference_features = _prepare_model_features(log_features)
    model_pipeline = _load_or_train_model(train_features)

    raw_predictions = model_pipeline.predict(inference_features)
    anomaly_scores = -model_pipeline.decision_function(inference_features)

    results = log_features.copy()
    results["anomaly_label"] = raw_predictions
    results["anomaly_score"] = anomaly_scores
    results["is_anomaly"] = results["anomaly_label"] == -1

    return results


def _load_or_train_model(train_features: pd.DataFrame) -> Pipeline:
    model_path = Path(ONE_CLASS_SVM_MODEL_PATH)

    if model_path.exists():
        return load(model_path)

    model_path.parent.mkdir(parents=True, exist_ok=True)

    training_features = _prepare_model_features(train_features)

    model_pipeline = _build_model_pipeline()
    model_pipeline.fit(training_features)

    dump(model_pipeline, model_path)
    return model_pipeline


def _build_model_pipeline() -> Pipeline:

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "parsed_text",
                TfidfVectorizer(max_features=3000, ngram_range=(1, 2)),
                TEXT_COLUMN,
            ),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_COLUMNS,
            ),
            (
                "numeric",
                StandardScaler(),
                NUMERIC_COLUMNS,
            ),
        ]
    )

    model = OneClassSVM(
        kernel="rbf",
        gamma="scale",
        nu=0.1,
    )

    return Pipeline(
        steps=[
            ("features", preprocessor),
            ("model", model),
        ]
    )


def _prepare_model_features(log_features: pd.DataFrame) -> pd.DataFrame:
    features = log_features.copy()

    features[TEXT_COLUMN] = _get_series(features, TEXT_COLUMN, "").fillna("").astype(str)
    if "tty" not in features.columns and "tt" in features.columns:
        features["tty"] = features["tt"]

    for column in CATEGORICAL_COLUMNS:
        features[column] = _get_series(features, column, "").fillna("").astype(str)

    features["day"] = pd.to_numeric(_get_series(features, "day", 0), errors="coerce").fillna(0)

    features["process_pid"] = pd.to_numeric(
        _get_series(features, "process_pid", -1),
        errors="coerce",
    ).fillna(-1)

    features["parameter_count"] = _get_series(features, "parameter_list", "").apply(
        _count_parameters
    )

    features["logline_length"] = _get_series(features, "logline", "").fillna("").astype(str).str.len()

    features["parsed_logline_length"] = features[TEXT_COLUMN].str.len()

    return features[[TEXT_COLUMN] + CATEGORICAL_COLUMNS + NUMERIC_COLUMNS]


def _get_series(frame: pd.DataFrame, column: str, default_value) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default_value] * len(frame), index=frame.index)


def _count_parameters(parameter_list) -> int:
    if isinstance(parameter_list, (list, tuple)):
        return len(parameter_list)

    if hasattr(parameter_list, "tolist") and not isinstance(parameter_list, str):
        val = parameter_list.tolist()
        if isinstance(val, list):
            return len(val)

    if parameter_list is None or not isinstance(parameter_list, str):
        return 0

    if pd.isna(parameter_list):
        return 0

    stripped = parameter_list.strip()
    if not stripped:
        return 0

    try:
        parsed = ast.literal_eval(stripped)
    except (ValueError, SyntaxError):
        return 0

    return len(parsed) if isinstance(parsed, list) else 0
