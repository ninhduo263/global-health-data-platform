# -*- coding: utf-8 -*-
"""
Phát hiện Điểm Bất thường Dịch tễ (Anomaly Detection — Isolation Forest)
trên chuỗi dữ liệu tuần–quốc gia (dwh.fact_weekly_covid_cluster).

Phân loại tự động 3 nhóm bất thường tiếng Việt:
  - Đột biến ca mắc cục bộ
  - Tử vong tăng dị thường
  - Nhiễu dồn / hiệu chỉnh báo cáo
"""

from datetime import date
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def _norm_date(val) -> date:
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val)[:10])


def _classify_anomaly_vi(
    cases_per_100k: float,
    deaths_per_100k: float,
    case_z: float,
    death_z: float,
    cfr: float,
    growth_pct,
) -> tuple[str, str]:
    if death_z >= 2.5 or (deaths_per_100k >= 5.0 and cfr >= 0.05):
        return (
            "Tử vong tăng dị thường",
            f"Tỷ lệ tử vong {deaths_per_100k:.2f}/100k dân (Z={death_z:+.1f}, CFR={cfr * 100:.1f}%) vượt ngưỡng bất thường.",
        )
    if case_z >= 2.2 and cases_per_100k >= 50.0:
        g_txt = f", tăng {growth_pct:+.0f}%/tuần" if growth_pct is not None and np.isfinite(growth_pct) else ""
        return (
            "Đột biến ca mắc cục bộ",
            f"Số ca mắc vọt lên {cases_per_100k:.1f}/100k dân (Z={case_z:+.1f} so với trung bình 4 tuần{g_txt}).",
        )
    return (
        "Nhiễu dồn / hiệu chỉnh báo cáo",
        f"Biến động bất thường giữa ca mắc ({cases_per_100k:.1f}/100k) và tử vong ({deaths_per_100k:.2f}/100k), nghi ngờ dồn báo cáo.",
    )


def detect_weekly_anomalies(
    records: list[dict],
    contamination: float = 0.02,
    random_state: int = 42,
    max_anomalies: int = 1500,
) -> list[dict]:
    """
    Run Isolation Forest on country-week epidemiological features and return ranked anomalies.
    """
    if not records:
        raise ValueError("No weekly records provided for anomaly detection")

    df = pd.DataFrame(records).copy()
    df["week_start"] = df["week_start"].map(_norm_date)
    df["country_code"] = df["country_code"].astype(str).str.strip()
    df["country_name"] = df.get("country_name", df["country_code"]).fillna(df["country_code"]).astype(str)
    df["cases_per_100k"] = pd.to_numeric(df["cases_per_100k"], errors="coerce").fillna(0.0).clip(lower=0.0)
    df["deaths_per_100k"] = pd.to_numeric(df["deaths_per_100k"], errors="coerce").fillna(0.0).clip(lower=0.0)
    df["case_growth_pct"] = pd.to_numeric(df.get("case_growth_pct"), errors="coerce")

    df = df.sort_values(["country_code", "week_start"]).reset_index(drop=True)

    frames = []
    for _, grp in df.groupby("country_code", sort=False):
        g = grp.sort_values("week_start").copy()
        log_c = np.log1p(g["cases_per_100k"].to_numpy(dtype=float))
        log_d = np.log1p(g["deaths_per_100k"].to_numpy(dtype=float))
        g["log_cases"] = log_c
        g["log_deaths"] = log_d

        roll_c_mean = g["log_cases"].rolling(4, min_periods=1).mean().shift(1).fillna(log_c[0])
        roll_c_std = g["log_cases"].rolling(4, min_periods=1).std(ddof=0).shift(1).fillna(0.25).clip(lower=0.25)
        g["case_z"] = (g["log_cases"] - roll_c_mean) / roll_c_std

        roll_d_mean = g["log_deaths"].rolling(4, min_periods=1).mean().shift(1).fillna(log_d[0])
        roll_d_std = g["log_deaths"].rolling(4, min_periods=1).std(ddof=0).shift(1).fillna(0.25).clip(lower=0.25)
        g["death_z"] = (g["log_deaths"] - roll_d_mean) / roll_d_std

        g["cfr_weekly"] = np.where(
            g["cases_per_100k"] > 0.0,
            np.clip(g["deaths_per_100k"] / np.maximum(g["cases_per_100k"], 1e-6), 0.0, 1.0),
            0.0,
        )
        frames.append(g)

    feat_df = pd.concat(frames, ignore_index=True)
    feature_cols = ["log_cases", "log_deaths", "case_z", "death_z", "cfr_weekly"]
    X = StandardScaler().fit_transform(feat_df[feature_cols].to_numpy(dtype=float))

    eff_contam = float(np.clip(contamination, 0.001, 0.25))
    iso = IsolationForest(
        n_estimators=200,
        contamination=eff_contam,
        random_state=random_state,
        n_jobs=2,
    )
    preds = iso.fit_predict(X)  # -1 = anomaly, 1 = normal
    raw_scores = -iso.decision_function(X)  # càng dương càng bất thường

    feat_df["is_anomaly"] = preds == -1
    feat_df["anomaly_score"] = raw_scores

    flagged = feat_df[feat_df["is_anomaly"]].sort_values("anomaly_score", ascending=False)
    if max_anomalies and len(flagged) > int(max_anomalies):
        flagged = flagged.head(int(max_anomalies))

    anomalies = []
    for row in flagged.itertuples(index=False):
        growth_val = None if pd.isna(row.case_growth_pct) else float(row.case_growth_pct)
        atype_vi, expl_vi = _classify_anomaly_vi(
            float(row.cases_per_100k),
            float(row.deaths_per_100k),
            float(row.case_z),
            float(row.death_z),
            float(row.cfr_weekly),
            growth_val,
        )
        anomalies.append(
            {
                "week_start": row.week_start,
                "country_code": str(row.country_code),
                "country_name": str(row.country_name),
                "cases_per_100k": float(row.cases_per_100k),
                "deaths_per_100k": float(row.deaths_per_100k),
                "case_growth_pct": growth_val,
                "case_z_score": float(row.case_z),
                "death_z_score": float(row.death_z),
                "anomaly_score": float(row.anomaly_score),
                "anomaly_type_vi": atype_vi,
                "explanation_vi": expl_vi,
            }
        )

    return anomalies
