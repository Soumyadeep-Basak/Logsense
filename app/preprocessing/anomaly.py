import pandas as pd
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import OneClassSVM

from app.config import ANOMALIES_PATH


def detect_anomalies(
    log_features: pd.DataFrame,
    train_features: pd.DataFrame | None = None,
    algo_name: str = "one_class_svm",
    output_path: Path = ANOMALIES_PATH,
) -> pd.DataFrame:
    """Train an anomaly detector, save predictions, and return them."""

    if algo_name != "one_class_svm":
        raise ValueError("Only 'one_class_svm' is currently supported.")

    if "parsed_logline" not in log_features.columns:
        raise KeyError("Expected a 'parsed_logline' column in parsed log features.")

    if train_features is None:
        train_features = log_features

    vectorizer = TfidfVectorizer()
    train_text = train_features["parsed_logline"].fillna("").astype(str)
    test_text = log_features["parsed_logline"].fillna("").astype(str)

    train_matrix = vectorizer.fit_transform(train_text)
    test_matrix = vectorizer.transform(test_text)

    detector = OneClassSVM(gamma="scale", nu=0.1)
    detector.fit(train_matrix)

    results = log_features.copy()
    results["anomaly_label"] = detector.predict(test_matrix)
    results["anomaly_score"] = detector.decision_function(test_matrix)
    results["is_anomaly"] = results["anomaly_label"] == -1
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(output_path, index=False)
    return results
