import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from weekly_covid_kmeans import (
    cluster_weekly_records,
    evaluate_cluster_counts,
    prepare_weekly_features,
    run_weekly_clustering,
    select_cluster_count,
)


class WeeklyCovidKMeansTests(unittest.TestCase):
    def setUp(self):
        self.records = [
            {
                "week_start": "2021-01-04",
                "country_code": "AAA",
                "country_name": "Low A",
                "population": 1_000_000,
                "weekly_cases": 100,
                "weekly_deaths": 1,
                "previous_week_cases": 50,
            },
            {
                "week_start": "2021-01-04",
                "country_code": "BBB",
                "country_name": "Low B",
                "population": 1_000_000,
                "weekly_cases": 120,
                "weekly_deaths": 2,
                "previous_week_cases": 60,
            },
            {
                "week_start": "2021-01-04",
                "country_code": "CCC",
                "country_name": "High A",
                "population": 1_000_000,
                "weekly_cases": 10_000,
                "weekly_deaths": 100,
                "previous_week_cases": 5_000,
            },
            {
                "week_start": "2021-01-04",
                "country_code": "DDD",
                "country_name": "High B",
                "population": 1_000_000,
                "weekly_cases": 11_000,
                "weekly_deaths": 120,
                "previous_week_cases": 5_500,
            },
        ]

        # 3 cụm tách biệt rõ rệt (mỗi cụm 5 điểm) để kiểm định Elbow / Silhouette / Auto-K
        self.three_cluster_records = []
        for idx in range(5):
            # Cụm 0: ca mắc rất thấp, ổn định
            self.three_cluster_records.append(
                {
                    "week_start": f"2021-01-{idx + 1:02d}",
                    "country_code": f"L{idx:02d}",
                    "country_name": f"Low {idx}",
                    "population": 1_000_000,
                    "weekly_cases": 20 + idx,
                    "weekly_deaths": 0,
                    "previous_week_cases": 20 + idx,
                }
            )
            # Cụm 1: ca mắc trung bình, đang giảm mạnh
            self.three_cluster_records.append(
                {
                    "week_start": f"2021-01-{idx + 1:02d}",
                    "country_code": f"M{idx:02d}",
                    "country_name": f"Mid {idx}",
                    "population": 1_000_000,
                    "weekly_cases": 800 + idx * 10,
                    "weekly_deaths": 8,
                    "previous_week_cases": 2_400 + idx * 20,
                }
            )
            # Cụm 2: ca mắc rất cao, đang tăng vọt
            self.three_cluster_records.append(
                {
                    "week_start": f"2021-01-{idx + 1:02d}",
                    "country_code": f"H{idx:02d}",
                    "country_name": f"High {idx}",
                    "population": 1_000_000,
                    "weekly_cases": 25_000 + idx * 100,
                    "weekly_deaths": 300 + idx * 5,
                    "previous_week_cases": 5_000 + idx * 20,
                }
            )

    def test_rates_are_normalized_and_clusters_are_ordered(self):
        clustered = cluster_weekly_records(self.records, requested_clusters=2)

        self.assertEqual(clustered[0]["cases_per_100k"], 10)
        self.assertEqual(clustered[0]["case_growth_pct"], 100)
        self.assertLess(clustered[0]["cluster_id"], clustered[2]["cluster_id"])
        self.assertIn("pca_1", clustered[0])
        self.assertIn("pca_2", clustered[0])
        self.assertIn("cluster_label", clustered[0])

    def test_same_input_produces_same_cluster_labels(self):
        first = cluster_weekly_records(self.records)
        second = cluster_weekly_records(self.records)

        self.assertEqual(
            [record["cluster_id"] for record in first],
            [record["cluster_id"] for record in second],
        )

    def test_invalid_population_is_skipped(self):
        records = self.records + [{**self.records[0], "country_code": "BAD", "population": 0}]

        clustered = cluster_weekly_records(records)

        self.assertEqual(len(clustered), len(self.records))

    def test_no_valid_population_raises(self):
        with self.assertRaisesRegex(ValueError, "valid population"):
            cluster_weekly_records([{"country_code": "BAD", "population": 0}])

    def test_evaluations_cover_k_range_and_have_single_selected_row(self):
        result = run_weekly_clustering(
            self.three_cluster_records,
            requested_clusters=4,
            k_range="2-6",
        )
        evaluations = result["evaluations"]

        self.assertEqual([row["k"] for row in evaluations], [2, 3, 4, 5, 6])
        selected_rows = [row for row in evaluations if row["is_selected"]]
        self.assertEqual(len(selected_rows), 1)
        self.assertEqual(selected_rows[0]["k"], 4)
        for row in evaluations:
            self.assertIsNotNone(row["inertia"])
            self.assertIsNotNone(row["silhouette"])
            self.assertIsNotNone(row["davies_bouldin"])
            self.assertIsNotNone(row["calinski_harabasz"])

    def test_auto_selects_k_with_highest_silhouette(self):
        result = run_weekly_clustering(
            self.three_cluster_records,
            requested_clusters="auto",
            k_range="2-6",
        )
        evaluations = result["evaluations"]
        best_sil_row = max(evaluations, key=lambda r: r["silhouette"])

        self.assertEqual(result["cluster_count"], best_sil_row["k"])
        self.assertEqual(result["cluster_count"], 3)
        self.assertTrue(best_sil_row["is_best_silhouette"])
        self.assertTrue(best_sil_row["is_selected"])

    def test_k_larger_than_distinct_points_is_skipped_without_crashing(self):
        # 6 bản ghi nhưng chỉ có 3 điểm đặc trưng khác nhau
        duplicated = self.records[:3] + [dict(r, country_code=f"{r['country_code']}2") for r in self.records[:3]]
        _, X_scaled, _ = prepare_weekly_features(duplicated)
        evaluations = evaluate_cluster_counts(X_scaled, k_values=range(2, 9))

        self.assertEqual([row["k"] for row in evaluations], [2, 3])
        chosen = select_cluster_count(evaluations, requested_clusters=8, distinct_rows=3)
        self.assertEqual(chosen, 3)

    def test_cluster_profiles_and_labels_are_ordered_by_incidence_and_trend(self):
        result = run_weekly_clustering(
            self.three_cluster_records,
            requested_clusters=3,
            k_range="2-5",
        )
        profiles = result["profiles"]

        self.assertEqual([p["cluster_id"] for p in profiles], [0, 1, 2])
        self.assertLess(profiles[0]["avg_cases_per_100k"], profiles[1]["avg_cases_per_100k"])
        self.assertLess(profiles[1]["avg_cases_per_100k"], profiles[2]["avg_cases_per_100k"])
        self.assertEqual(profiles[0]["cluster_label"], "Thấp – ổn định")
        self.assertEqual(profiles[1]["cluster_label"], "Trung bình – đang giảm")
        self.assertEqual(profiles[2]["cluster_label"], "Cao – đang tăng")
        self.assertAlmostEqual(sum(p["member_share"] for p in profiles), 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
