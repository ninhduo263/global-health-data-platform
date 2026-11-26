from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from xgboost import XGBClassifier

    HAS_XGBOOST = True
except Exception:  # pragma: no cover - fallback when xgboost wheel is unavailable
    XGBClassifier = None
    HAS_XGBOOST = False


BASE_FEATURE_COLUMNS = (
    "log_cases_per_100k",
    "log_cases_lag_1",
    "log_cases_lag_2",
    "log_cases_lag_3",
    "cases_roll4_mean",
    "cases_roll4_std",
    "growth_1w",
    "growth_2w",
    "growth_accel",
    "log_deaths_per_100k",
    "death_growth_1w",
    "cfr_roll4",
    "vax_any_pct",
    "vax_full_pct",
    "week_sin",
    "week_cos",
    "global_mean_growth_1w",
)


def get_feature_columns(include_cluster: bool = True) -> list[str]:
    cols = list(BASE_FEATURE_COLUMNS)
    if include_cluster:
        cols.append("cluster_id")
    return cols


class _ConstantProbClassifier:
    """Fallback classifier when training data contains only a single class."""

    def __init__(self, constant_prob: float):
        self.constant_prob = float(np.clip(constant_prob, 0.0, 1.0))

    def fit(self, X, y):
        return self

    def predict_proba(self, X) -> np.ndarray:
        n = len(X)
        p1 = np.full(n, self.constant_prob, dtype=float)
        return np.column_stack([1.0 - p1, p1])


def _normalize_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def build_feature_frame(
    records,
    horizon_weeks: int = 2,
    surge_ratio: float = 1.5,
    min_cases_per_100k: float = 10.0,
    include_cluster: bool = True,
    drop_incomplete_latest_week: bool = True,
) -> pd.DataFrame:
    """
    Build strictly causal country-week features (using only data up to week t)
    and compute binary surge target at week t + horizon_weeks.
    """
    if horizon_weeks < 1:
        raise ValueError("horizon_weeks must be at least 1")
    if surge_ratio <= 0:
        raise ValueError("surge_ratio must be positive")
    if min_cases_per_100k < 0:
        raise ValueError("min_cases_per_100k must be non-negative")

    df = pd.DataFrame(records).copy()
    if df.empty:
        raise ValueError("No weekly COVID-19 records provided for early warning")

    df["week_start"] = df["week_start"].map(_normalize_date)
    df["country_code"] = df["country_code"].astype(str).str.strip()
    if "country_name" not in df.columns:
        df["country_name"] = df["country_code"]
    else:
        df["country_name"] = df["country_name"].fillna(df["country_code"]).astype(str)

    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df[np.isfinite(df["population"]) & (df["population"] > 0)].copy()
    if df.empty:
        raise ValueError("No weekly records with valid population for early warning")

    if "weekly_cases" in df.columns:
        df["weekly_cases"] = pd.to_numeric(df["weekly_cases"], errors="coerce").fillna(0.0).clip(lower=0.0)
    else:
        df["weekly_cases"] = 0.0

    if "weekly_deaths" in df.columns:
        df["weekly_deaths"] = pd.to_numeric(df["weekly_deaths"], errors="coerce").fillna(0.0).clip(lower=0.0)
    else:
        df["weekly_deaths"] = 0.0

    if "cases_per_100k" in df.columns:
        df["cases_per_100k"] = pd.to_numeric(df["cases_per_100k"], errors="coerce")
        missing_rate = df["cases_per_100k"].isna()
        df.loc[missing_rate, "cases_per_100k"] = (
            df.loc[missing_rate, "weekly_cases"] * 100_000.0 / df.loc[missing_rate, "population"]
        )
    else:
        df["cases_per_100k"] = df["weekly_cases"] * 100_000.0 / df["population"]
    df["cases_per_100k"] = df["cases_per_100k"].fillna(0.0).clip(lower=0.0)

    if "deaths_per_100k" in df.columns:
        df["deaths_per_100k"] = pd.to_numeric(df["deaths_per_100k"], errors="coerce")
        missing_drate = df["deaths_per_100k"].isna()
        df.loc[missing_drate, "deaths_per_100k"] = (
            df.loc[missing_drate, "weekly_deaths"] * 100_000.0 / df.loc[missing_drate, "population"]
        )
    else:
        df["deaths_per_100k"] = df["weekly_deaths"] * 100_000.0 / df["population"]
    df["deaths_per_100k"] = df["deaths_per_100k"].fillna(0.0).clip(lower=0.0)

    for vax_col in ("people_vaccinated", "people_fully_vaccinated"):
        if vax_col in df.columns:
            df[vax_col] = pd.to_numeric(df[vax_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        else:
            df[vax_col] = 0.0

    if "cluster_id" in df.columns:
        df["cluster_id"] = pd.to_numeric(df["cluster_id"], errors="coerce").fillna(0).astype(int)
    else:
        df["cluster_id"] = 0

    if "observed_days" in df.columns:
        df["observed_days"] = pd.to_numeric(df["observed_days"], errors="coerce").fillna(7).astype(int)
    else:
        df["observed_days"] = 7

    df = df.sort_values(["country_code", "week_start"]).reset_index(drop=True)

    # Loại tuần cuối cùng của mỗi quốc gia nếu chưa đủ 7 ngày để tránh số ca thấp giả tạo
    if drop_incomplete_latest_week:
        max_week_by_country = df.groupby("country_code")["week_start"].transform("max")
        is_incomplete_tail = (df["week_start"] == max_week_by_country) & (df["observed_days"] < 7)
        df = df[~is_incomplete_tail].reset_index(drop=True)
        if df.empty:
            raise ValueError("All weekly records were filtered out as incomplete weeks")

    # Tính đặc trưng theo từng quốc gia (chỉ dùng dữ liệu <= tuần t)
    country_frames = []
    for _, grp in df.groupby("country_code", sort=False):
        g = grp.sort_values("week_start").copy()
        log_cases = np.log1p(g["cases_per_100k"].to_numpy(dtype=float))
        log_deaths = np.log1p(g["deaths_per_100k"].to_numpy(dtype=float))

        g["log_cases_per_100k"] = log_cases
        first_log_case = float(log_cases[0])
        g["log_cases_lag_1"] = g["log_cases_per_100k"].shift(1).fillna(first_log_case)
        g["log_cases_lag_2"] = g["log_cases_per_100k"].shift(2).fillna(first_log_case)
        g["log_cases_lag_3"] = g["log_cases_per_100k"].shift(3).fillna(first_log_case)

        g["cases_roll4_mean"] = g["log_cases_per_100k"].rolling(4, min_periods=1).mean()
        g["cases_roll4_std"] = (
            g["log_cases_per_100k"].rolling(4, min_periods=1).std(ddof=0).fillna(0.0)
        )

        g["growth_1w"] = g["log_cases_per_100k"] - g["log_cases_lag_1"]
        g["growth_2w"] = g["log_cases_per_100k"] - g["log_cases_lag_2"]
        g["growth_accel"] = (g["growth_1w"] - g["growth_1w"].shift(1)).fillna(0.0)

        g["log_deaths_per_100k"] = log_deaths
        first_log_death = float(log_deaths[0])
        g["death_growth_1w"] = (
            g["log_deaths_per_100k"] - g["log_deaths_per_100k"].shift(1).fillna(first_log_death)
        )

        roll4_deaths = g["deaths_per_100k"].rolling(4, min_periods=1).sum()
        roll4_cases = g["cases_per_100k"].rolling(4, min_periods=1).sum()
        g["cfr_roll4"] = np.where(
            roll4_cases > 0.0,
            np.clip(roll4_deaths / np.maximum(roll4_cases, 1e-9), 0.0, 1.0),
            0.0,
        )

        g["vax_any_pct"] = np.clip(
            g["people_vaccinated"].cummax() / g["population"], 0.0, 1.0
        )
        g["vax_full_pct"] = np.clip(
            g["people_fully_vaccinated"].cummax() / g["population"], 0.0, 1.0
        )

        prev_rate = g["cases_per_100k"].shift(1)
        computed_growth_pct = np.where(
            prev_rate.notna() & (prev_rate > 0.0),
            (g["cases_per_100k"] - prev_rate) / prev_rate * 100.0,
            np.nan,
        )
        if "case_growth_pct" in g.columns:
            existing_pct = pd.to_numeric(g["case_growth_pct"], errors="coerce")
            g["case_growth_pct"] = existing_pct.where(existing_pct.notna(), computed_growth_pct)
        else:
            g["case_growth_pct"] = computed_growth_pct

        # Ánh xạ nhãn ở đúng tuần t + H (không dùng dữ liệu t+H cho feature tại t)
        rate_by_week = dict(zip(g["week_start"], g["cases_per_100k"]))
        max_week = g["week_start"].iloc[-1]
        horizon_delta = timedelta(days=7 * int(horizon_weeks))
        forecast_cutoff = max_week - horizon_delta

        future_rates = []
        actual_surges = []
        is_forecast = []
        for wk, cur_rate in zip(g["week_start"], g["cases_per_100k"]):
            target_wk = wk + horizon_delta
            if target_wk in rate_by_week:
                fut_rate = float(rate_by_week[target_wk])
                surge = bool(
                    fut_rate >= float(surge_ratio) * float(cur_rate)
                    and fut_rate >= float(min_cases_per_100k)
                )
                future_rates.append(fut_rate)
                actual_surges.append(surge)
                is_forecast.append(False)
            elif wk > forecast_cutoff:
                future_rates.append(np.nan)
                actual_surges.append(None)
                is_forecast.append(True)
            else:
                future_rates.append(np.nan)
                actual_surges.append(None)
                is_forecast.append(False)

        g["future_cases_per_100k"] = future_rates
        g["actual_surge"] = pd.Series(actual_surges, index=g.index, dtype="object")
        g["is_forecast_horizon"] = is_forecast
        country_frames.append(g)

    result = pd.concat(country_frames, ignore_index=True)

    iso_weeks = pd.to_datetime(result["week_start"]).dt.isocalendar().week.astype(float)
    result["week_sin"] = np.sin(2.0 * np.pi * iso_weeks / 52.0)
    result["week_cos"] = np.cos(2.0 * np.pi * iso_weeks / 52.0)

    # Bối cảnh toàn cầu trong cùng tuần t (không chạm dữ liệu tương lai > t)
    result["global_mean_growth_1w"] = result.groupby("week_start")["growth_1w"].transform("mean")

    if not include_cluster and "cluster_id" in result.columns:
        result["cluster_id_raw"] = result["cluster_id"]

    return result.sort_values(["week_start", "country_code"]).reset_index(drop=True)


def purged_time_split(
    df: pd.DataFrame,
    horizon_weeks: int = 2,
    test_fraction: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, date]:
    """
    Split labeled country-week observations chronologically with an H-week purge gap:
      - train: week_start < cutoff - H weeks
      - test:  week_start >= cutoff
      - forecast: trailing H weeks per country without ground-truth labels
    """
    if not (0.0 < float(test_fraction) < 0.9):
        raise ValueError("test_fraction must be between 0 and 0.9")

    forecast_df = df[df["is_forecast_horizon"]].copy()
    forecast_df["prediction_split"] = "forecast"

    labeled_df = df[df["actual_surge"].notna()].copy()
    unique_weeks = sorted(labeled_df["week_start"].unique())
    if len(unique_weeks) <= int(horizon_weeks) + 1:
        raise ValueError(
            f"Need more than {horizon_weeks + 1} labeled weeks for purged split, got {len(unique_weeks)}"
        )

    n_weeks = len(unique_weeks)
    desired_test_weeks = max(1, int(round(n_weeks * float(test_fraction))))
    horizon_delta = timedelta(days=7 * int(horizon_weeks))

    # Đảm bảo sau khi trừ khoảng purge H tuần vẫn còn ít nhất 1 tuần train
    cutoff = None
    for n_test in range(desired_test_weeks, 0, -1):
        cand_cutoff = unique_weeks[n_weeks - n_test]
        cand_purge_limit = cand_cutoff - horizon_delta
        if any(wk < cand_purge_limit for wk in unique_weeks):
            cutoff = cand_cutoff
            break

    if cutoff is None:
        raise ValueError("Unable to construct a non-empty purged train/test time split")

    purge_limit = cutoff - horizon_delta
    train_df = labeled_df[labeled_df["week_start"] < purge_limit].copy()
    test_df = labeled_df[labeled_df["week_start"] >= cutoff].copy()
    train_df["prediction_split"] = "train"
    test_df["prediction_split"] = "test"

    return train_df, test_df, forecast_df, cutoff


def _select_best_f1_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    if len(y_true) == 0 or int(np.sum(y_true == 1)) == 0:
        return 0.5

    grid = np.linspace(0.10, 0.90, 81)
    quantiles = np.quantile(y_prob, np.linspace(0.05, 0.95, 19))
    candidates = np.unique(np.clip(np.concatenate([grid, quantiles]), 0.05, 0.95))

    best_threshold = 0.5
    best_f1 = -1.0
    for thresh in candidates:
        preds = (y_prob >= thresh).astype(int)
        score = float(f1_score(y_true, preds, zero_division=0))
        if score > best_f1 or (
            score == best_f1 and abs(thresh - 0.5) < abs(best_threshold - 0.5)
        ):
            best_f1 = score
            best_threshold = float(thresh)

    return best_threshold if best_f1 > 0.0 else 0.5


def fit_with_threshold(
    estimator_factory,
    train_df: pd.DataFrame,
    feature_cols: list[str],
    horizon_weeks: int = 2,
    val_fraction: float = 0.15,
) -> tuple[object, float]:
    """
    Tune decision threshold on the last val_fraction weeks of train_df by max F1,
    then refit on all of train_df. Safe when train_df contains only a single class.
    """
    X_all = train_df[feature_cols].to_numpy(dtype=float)
    y_all = train_df["actual_surge"].astype(int).to_numpy()

    unique_classes = np.unique(y_all)
    if len(unique_classes) < 2:
        const_model = _ConstantProbClassifier(constant_prob=float(unique_classes[0])).fit(
            X_all, y_all
        )
        return const_model, 0.5

    train_weeks = sorted(train_df["week_start"].unique())
    threshold = 0.5
    if len(train_weeks) >= 4:
        n_val_weeks = max(1, int(round(len(train_weeks) * float(val_fraction))))
        val_cutoff = train_weeks[-n_val_weeks]
        purge_limit = val_cutoff - timedelta(days=7 * int(horizon_weeks))

        inner_train = train_df[train_df["week_start"] < purge_limit]
        if inner_train.empty:
            inner_train = train_df[train_df["week_start"] < val_cutoff]
        val_part = train_df[train_df["week_start"] >= val_cutoff]

        y_inner = inner_train["actual_surge"].astype(int).to_numpy()
        y_val = val_part["actual_surge"].astype(int).to_numpy()
        if not inner_train.empty and not val_part.empty and len(np.unique(y_inner)) >= 2:
            val_estimator = estimator_factory(y_inner)
            val_estimator.fit(inner_train[feature_cols].to_numpy(dtype=float), y_inner)
            val_probs = val_estimator.predict_proba(
                val_part[feature_cols].to_numpy(dtype=float)
            )[:, 1]
            threshold = _select_best_f1_threshold(y_val, val_probs)

    final_estimator = estimator_factory(y_all)
    final_estimator.fit(X_all, y_all)
    return final_estimator, float(threshold)


def _make_primary_estimator_factory(random_state: int = 42):
    if HAS_XGBOOST:
        model_name = "xgboost"

        def factory(y_train: np.ndarray):
            n_pos = int(np.sum(y_train == 1))
            n_neg = int(np.sum(y_train == 0))
            scale_pos_weight = float(n_neg / n_pos) if (n_pos > 0 and n_neg > 0) else 1.0
            return XGBClassifier(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                scale_pos_weight=scale_pos_weight,
                eval_metric="logloss",
                random_state=random_state,
                n_jobs=2,
            )

        return model_name, factory

    model_name = "hist_gradient_boosting"

    def fallback_factory(_y_train: np.ndarray):
        return HistGradientBoostingClassifier(
            max_iter=300,
            max_depth=4,
            learning_rate=0.05,
            class_weight="balanced",
            random_state=random_state,
        )

    return model_name, fallback_factory


def _make_logistic_factory(random_state: int = 42):
    def factory(_y_train: np.ndarray):
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        class_weight="balanced",
                        max_iter=1000,
                        random_state=random_state,
                    ),
                ),
            ]
        )

    return factory


def _rule_growth_scores(df: pd.DataFrame) -> np.ndarray:
    """
    Baseline rule: alert when 1-week case growth >= 25%.
    Mapped smoothly onto [0, 1] via logistic transform centered at 25% so score >= 0.5
    iff growth >= 25%, while preserving rank order for ROC-AUC / PR-AUC.
    """
    growth = pd.to_numeric(df["case_growth_pct"], errors="coerce").fillna(-100.0).to_numpy(dtype=float)
    z = np.clip((growth - 25.0) / 25.0, -40.0, 40.0)
    return 1.0 / (1.0 + np.exp(-z))


def _compute_binary_metrics(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    threshold: float,
) -> dict:
    y_pred = (y_prob >= float(threshold)).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    has_two_classes = len(np.unique(y_true)) >= 2
    roc_auc: Optional[float] = None
    pr_auc: Optional[float] = None
    if has_two_classes:
        roc_auc = float(roc_auc_score(y_true, y_prob))
        pr_auc = float(average_precision_score(y_true, y_prob))

    return {
        "decision_threshold": float(threshold),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision_score": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall_score": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1_score": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "true_positive": int(tp),
        "false_positive": int(fp),
        "true_negative": int(tn),
        "false_negative": int(fn),
    }


def evaluate_models(
    trained_models: list[dict],
    test_df: pd.DataFrame,
    feature_cols: list[str],
    train_rows: int,
    horizon_weeks: int,
    surge_ratio: float,
    min_cases_per_100k: float,
) -> list[dict]:
    """Evaluate baseline rule, Logistic Regression, and primary GBDT on the same test set."""
    y_test = test_df["actual_surge"].astype(int).to_numpy()
    X_test = test_df[feature_cols].to_numpy(dtype=float)
    test_rows = int(len(test_df))
    pos_rate = float(np.mean(y_test)) if test_rows > 0 else None
    test_start = test_df["week_start"].min() if test_rows > 0 else None
    test_end = test_df["week_start"].max() if test_rows > 0 else None

    evaluations = []
    for item in trained_models:
        name = item["model_name"]
        thresh = float(item["threshold"])
        if item.get("estimator") is None:
            probs = _rule_growth_scores(test_df)
        else:
            probs = np.clip(item["estimator"].predict_proba(X_test)[:, 1], 0.0, 1.0)

        metrics = _compute_binary_metrics(y_test, probs, thresh)
        evaluations.append(
            {
                "model_name": name,
                "is_primary": bool(item.get("is_primary", False)),
                **metrics,
                "train_rows": int(train_rows),
                "test_rows": test_rows,
                "test_positive_rate": pos_rate,
                "test_start_week": test_start,
                "test_end_week": test_end,
                "horizon_weeks": int(horizon_weeks),
                "surge_ratio": float(surge_ratio),
                "min_cases_per_100k": float(min_cases_per_100k),
            }
        )

    return evaluations


def _classify_alert_level(prob: float, threshold: float) -> str:
    if prob >= threshold:
        return "HIGH"
    if prob >= threshold / 2.0:
        return "WATCH"
    return "LOW"


def predict_alerts(
    estimator,
    model_name: str,
    threshold: float,
    target_df: pd.DataFrame,
    feature_cols: list[str],
    horizon_weeks: int,
) -> list[dict]:
    """Generate alert predictions for test (backtest) and forecast (live horizon) splits."""
    if target_df.empty:
        return []

    X = target_df[feature_cols].to_numpy(dtype=float)
    probs = np.clip(estimator.predict_proba(X)[:, 1], 0.0, 1.0)
    thresh = float(threshold)

    alerts = []
    for row, prob_val in zip(target_df.itertuples(index=False), probs):
        prob = float(prob_val)
        pred_surge = bool(prob >= thresh)
        split_name = str(row.prediction_split)
        is_test = split_name == "test" and row.actual_surge is not None and not pd.isna(row.actual_surge)

        growth_pct = (
            None
            if pd.isna(row.case_growth_pct)
            else float(row.case_growth_pct)
        )
        fut_rate = (
            float(row.future_cases_per_100k)
            if is_test and not pd.isna(row.future_cases_per_100k)
            else None
        )
        actual = bool(row.actual_surge) if is_test else None
        cid = None if pd.isna(row.cluster_id) else int(row.cluster_id)

        alerts.append(
            {
                "week_start": row.week_start,
                "country_code": str(row.country_code),
                "country_name": str(row.country_name),
                "model_name": model_name,
                "horizon_weeks": int(horizon_weeks),
                "prediction_split": split_name,
                "cases_per_100k": float(row.cases_per_100k),
                "case_growth_pct": growth_pct,
                "cluster_id": cid,
                "surge_probability": prob,
                "decision_threshold": thresh,
                "predicted_surge": pred_surge,
                "alert_level": _classify_alert_level(prob, thresh),
                "actual_surge": actual,
                "future_cases_per_100k": fut_rate,
            }
        )

    return alerts


def extract_feature_importances(
    estimator,
    model_name: str,
    feature_cols: list[str],
    reference_df: pd.DataFrame,
    random_state: int = 42,
) -> list[dict]:
    """Extract ranked feature importances (gain for XGBoost; permutation fallback otherwise)."""
    if hasattr(estimator, "feature_importances_"):
        raw_imp = np.asarray(estimator.feature_importances_, dtype=float)
    elif isinstance(estimator, _ConstantProbClassifier) or reference_df.empty:
        raw_imp = np.zeros(len(feature_cols), dtype=float)
    else:
        X_ref = reference_df[feature_cols].to_numpy(dtype=float)
        y_ref = reference_df["actual_surge"].astype(int).to_numpy()
        perm = permutation_importance(
            estimator,
            X_ref,
            y_ref,
            n_repeats=5,
            random_state=random_state,
        )
        raw_imp = np.maximum(perm.importances_mean, 0.0)

    total = float(np.sum(raw_imp))
    norm_imp = raw_imp / total if total > 0.0 else raw_imp

    ordered_indices = sorted(
        range(len(feature_cols)),
        key=lambda idx: (-float(norm_imp[idx]), feature_cols[idx]),
    )
    return [
        {
            "model_name": model_name,
            "feature_name": feature_cols[idx],
            "importance": float(norm_imp[idx]),
            "importance_rank": rank,
        }
        for rank, idx in enumerate(ordered_indices, start=1)
    ]


def run_early_warning_pipeline(
    records,
    horizon_weeks: int = 2,
    surge_ratio: float = 1.5,
    min_cases_per_100k: float = 10.0,
    test_fraction: float = 0.2,
    include_cluster: bool = True,
    drop_incomplete_latest_week: bool = True,
    random_state: int = 42,
) -> dict:
    """End-to-end pure function pipeline for COVID-19 surge early warning."""
    feature_cols = get_feature_columns(include_cluster=include_cluster)
    feature_df = build_feature_frame(
        records,
        horizon_weeks=horizon_weeks,
        surge_ratio=surge_ratio,
        min_cases_per_100k=min_cases_per_100k,
        include_cluster=include_cluster,
        drop_incomplete_latest_week=drop_incomplete_latest_week,
    )
    train_df, test_df, forecast_df, cutoff = purged_time_split(
        feature_df,
        horizon_weeks=horizon_weeks,
        test_fraction=test_fraction,
    )

    lr_model, lr_thresh = fit_with_threshold(
        _make_logistic_factory(random_state=random_state),
        train_df,
        feature_cols,
        horizon_weeks=horizon_weeks,
    )

    primary_name, primary_factory = _make_primary_estimator_factory(random_state=random_state)
    primary_model, primary_thresh = fit_with_threshold(
        primary_factory,
        train_df,
        feature_cols,
        horizon_weeks=horizon_weeks,
    )

    trained_models = [
        {"model_name": "rule_growth_25pct", "estimator": None, "threshold": 0.5, "is_primary": False},
        {"model_name": "logistic_regression", "estimator": lr_model, "threshold": lr_thresh, "is_primary": False},
        {"model_name": primary_name, "estimator": primary_model, "threshold": primary_thresh, "is_primary": True},
    ]

    evaluations = evaluate_models(
        trained_models,
        test_df=test_df,
        feature_cols=feature_cols,
        train_rows=len(train_df),
        horizon_weeks=horizon_weeks,
        surge_ratio=surge_ratio,
        min_cases_per_100k=min_cases_per_100k,
    )

    scoring_df = pd.concat([test_df, forecast_df], ignore_index=True)
    predictions = predict_alerts(
        estimator=primary_model,
        model_name=primary_name,
        threshold=primary_thresh,
        target_df=scoring_df,
        feature_cols=feature_cols,
        horizon_weeks=horizon_weeks,
    )

    ref_df = test_df if len(np.unique(test_df["actual_surge"].astype(int))) >= 2 else train_df
    importances = extract_feature_importances(
        estimator=primary_model,
        model_name=primary_name,
        feature_cols=feature_cols,
        reference_df=ref_df,
        random_state=random_state,
    )

    return {
        "primary_model_name": primary_name,
        "cutoff_week": cutoff,
        "predictions": predictions,
        "evaluations": evaluations,
        "feature_importances": importances,
    }
