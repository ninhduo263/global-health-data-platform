import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from weekly_covid_kmeans import cluster_weekly_records


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

    def test_rates_are_normalized_and_clusters_are_ordered(self):
        clustered = cluster_weekly_records(self.records, requested_clusters=2)

        self.assertEqual(clustered[0]["cases_per_100k"], 10)
        self.assertEqual(clustered[0]["case_growth_pct"], 100)
        self.assertLess(clustered[0]["cluster_id"], clustered[2]["cluster_id"])

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


if __name__ == "__main__":
    unittest.main()