import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from covid_early_warning import (
    build_feature_frame,
    get_feature_columns,
    purged_time_split,
    run_early_warning_pipeline,
)


def _make_weekly_series(
    country_code: str,
    weekly_cases_list: list[int],
    start_date: date = date(2021, 1, 4),
    population: int = 100_000,
    cluster_id: int = 1,
) -> list[dict]:
    rows = []
    for idx, cases in enumerate(weekly_cases_list):
        wk = start_date + timedelta(days=7 * idx)
        rows.append(
            {
                "week_start": wk.isoformat(),
                "country_code": country_code,
                "country_name": f"Country {country_code}",
                "population": population,
                "weekly_cases": cases,
                "weekly_deaths": max(0, cases // 50),
                "cases_per_100k": float(cases),  # vì population = 100_000
                "deaths_per_100k": float(cases) / 50.0,
                "cluster_id": cluster_id,
                "people_vaccinated": min(population, idx * 2_000),
                "people_fully_vaccinated": min(population, idx * 1_500),
                "observed_days": 7,
            }
        )
    return rows


class CovidEarlyWarningTests(unittest.TestCase):
    def test_surge_label_matches_doubling_vs_flat_series(self):
        # Chuỗi tăng gấp đôi mỗi tuần -> nhãn = True; chuỗi đi ngang 20 ca/100k -> nhãn = False
        doubling = _make_weekly_series("DBL", [10 * (2**i) for i in range(8)])
        flat = _make_weekly_series("FLT", [20] * 8)

        df = build_feature_frame(
            doubling + flat,
            horizon_weeks=2,
            surge_ratio=1.5,
            min_cases_per_100k=10.0,
        )

        dbl_labeled = df[(df["country_code"] == "DBL") & df["actual_surge"].notna()]
        flt_labeled = df[(df["country_code"] == "FLT") & df["actual_surge"].notna()]

        self.assertEqual(len(dbl_labeled), 6)
        self.assertTrue(all(bool(v) is True for v in dbl_labeled["actual_surge"]))
        self.assertEqual(len(flt_labeled), 6)
        self.assertTrue(all(bool(v) is False for v in flt_labeled["actual_surge"]))

    def test_features_at_week_t_do_not_leak_future_data(self):
        base_series = _make_weekly_series("AAA", [15, 18, 22, 25, 30, 35, 40, 45])
        other_country = _make_weekly_series("BBB", [10, 12, 11, 13, 14, 15, 16, 18])

        df_before = build_feature_frame(base_series + other_country, horizon_weeks=2)

        # Thay đổi đột biến dữ liệu từ tuần index=4 (t+1 so với index=3) trở đi ở cả hai nước
        mutated_a = [dict(r) for r in base_series]
        mutated_b = [dict(r) for r in other_country]
        for idx in range(4, 8):
            mutated_a[idx]["weekly_cases"] = 99_999
            mutated_a[idx]["cases_per_100k"] = 99_999.0
            mutated_a[idx]["people_vaccinated"] = 99_000
            mutated_b[idx]["weekly_cases"] = 88_888
            mutated_b[idx]["cases_per_100k"] = 88_888.0

        df_after = build_feature_frame(mutated_a + mutated_b, horizon_weeks=2)

        target_week = date(2021, 1, 4) + timedelta(days=7 * 3)  # tuần t (index = 3)
        feature_cols = get_feature_columns(include_cluster=True)

        before_t = (
            df_before[df_before["week_start"] <= target_week]
            .sort_values(["week_start", "country_code"])[feature_cols]
            .to_numpy(dtype=float)
        )
        after_t = (
            df_after[df_after["week_start"] <= target_week]
            .sort_values(["week_start", "country_code"])[feature_cols]
            .to_numpy(dtype=float)
        )
        np.testing.assert_allclose(before_t, after_t, rtol=0, atol=1e-12)

    def test_purged_time_split_leaves_h_week_gap_between_train_and_test(self):
        horizon = 2
        records = _make_weekly_series("AAA", [20 + (i % 5) * 15 for i in range(20)])
        df = build_feature_frame(records, horizon_weeks=horizon)
        train_df, test_df, _, cutoff = purged_time_split(
            df,
            horizon_weeks=horizon,
            test_fraction=0.25,
        )

        max_train_week = train_df["week_start"].max()
        min_test_week = test_df["week_start"].min()

        self.assertEqual(min_test_week, cutoff)
        self.assertLess(max_train_week, cutoff - timedelta(weeks=horizon))

    def test_last_h_weeks_per_country_are_assigned_to_forecast_without_labels(self):
        horizon = 3
        records = (
            _make_weekly_series("AAA", [15 + i * 5 for i in range(18)])
            + _make_weekly_series("BBB", [30 + (i % 4) * 20 for i in range(18)])
        )
        result = run_early_warning_pipeline(
            records,
            horizon_weeks=horizon,
            test_fraction=0.25,
        )

        forecast_preds = [
            p for p in result["predictions"] if p["prediction_split"] == "forecast"
        ]
        self.assertEqual(len(forecast_preds), 2 * horizon)
        for country in ("AAA", "BBB"):
            c_rows = [p for p in forecast_preds if p["country_code"] == country]
            self.assertEqual(len(c_rows), horizon)
            for p in c_rows:
                self.assertIsNone(p["actual_surge"])
                self.assertIsNone(p["future_cases_per_100k"])
                self.assertIn(p["alert_level"], ("HIGH", "WATCH", "LOW"))

    def test_single_class_dataset_returns_none_auc_without_crashing(self):
        # Dữ liệu hoàn toàn đi ngang -> tất cả nhãn = False (chỉ có 1 lớp 0)
        flat_a = _make_weekly_series("AAA", [25] * 16)
        flat_b = _make_weekly_series("BBB", [30] * 16)

        result = run_early_warning_pipeline(
            flat_a + flat_b,
            horizon_weeks=2,
            test_fraction=0.25,
        )

        self.assertEqual(len(result["evaluations"]), 3)
        for ev in result["evaluations"]:
            self.assertIsNone(ev["roc_auc"])
            self.assertIsNone(ev["pr_auc"])
            self.assertEqual(ev["accuracy"], 1.0)
        self.assertTrue(len(result["predictions"]) > 0)
        self.assertEqual(
            len(result["feature_importances"]),
            len(get_feature_columns(include_cluster=True)),
        )


if __name__ == "__main__":
    unittest.main()
