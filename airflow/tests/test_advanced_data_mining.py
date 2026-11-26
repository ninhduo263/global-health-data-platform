import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from covid_anomaly_detection import detect_weekly_anomalies
from covid_cluster_transitions import (
    compute_cluster_dwell_times,
    compute_cluster_transitions,
)
from patient_association_rules import mine_patient_association_rules


class ClusterTransitionTests(unittest.TestCase):
    def setUp(self):
        # Quốc gia AAA: 0 -> 0 -> 1 -> 2 -> 2 -> 2 -> 1
        # Quốc gia BBB: 0 -> 1 -> 1 -> 0
        self.records = [
            {"week_start": "2021-01-04", "country_code": "AAA", "cluster_id": 0, "cluster_label": "Thấp – ổn định"},
            {"week_start": "2021-01-11", "country_code": "AAA", "cluster_id": 0, "cluster_label": "Thấp – ổn định"},
            {"week_start": "2021-01-18", "country_code": "AAA", "cluster_id": 1, "cluster_label": "Trung bình – ổn định"},
            {"week_start": "2021-01-25", "country_code": "AAA", "cluster_id": 2, "cluster_label": "Cao – đang tăng"},
            {"week_start": "2021-02-01", "country_code": "AAA", "cluster_id": 2, "cluster_label": "Cao – đang tăng"},
            {"week_start": "2021-02-08", "country_code": "AAA", "cluster_id": 2, "cluster_label": "Cao – đang tăng"},
            {"week_start": "2021-02-15", "country_code": "AAA", "cluster_id": 1, "cluster_label": "Trung bình – ổn định"},
            {"week_start": "2021-01-04", "country_code": "BBB", "cluster_id": 0, "cluster_label": "Thấp – ổn định"},
            {"week_start": "2021-01-11", "country_code": "BBB", "cluster_id": 1, "cluster_label": "Trung bình – ổn định"},
            {"week_start": "2021-01-18", "country_code": "BBB", "cluster_id": 1, "cluster_label": "Trung bình – ổn định"},
            {"week_start": "2021-01-25", "country_code": "BBB", "cluster_id": 0, "cluster_label": "Thấp – ổn định"},
        ]

    def test_transition_probabilities_sum_to_one_per_source_cluster(self):
        transitions = compute_cluster_transitions(self.records)
        for source_cid in (0, 1, 2):
            outgoing = [t for t in transitions if t["from_cluster_id"] == source_cid]
            self.assertEqual(len(outgoing), 3)
            prob_sum = sum(t["transition_probability"] for t in outgoing)
            self.assertAlmostEqual(prob_sum, 1.0, places=6)

        # Từ cụm 0 có 3 bước chuyển: (0->0), (0->1) ở AAA và (0->1) ở BBB => P(0->1) = 2/3
        t_0_1 = next(
            t for t in transitions if t["from_cluster_id"] == 0 and t["to_cluster_id"] == 1
        )
        self.assertEqual(t_0_1["transition_count"], 2)
        self.assertAlmostEqual(t_0_1["transition_probability"], 2.0 / 3.0, places=6)
        self.assertEqual(t_0_1["transition_type_vi"], "Leo thang (Dịch nặng lên)")

    def test_dwell_times_capture_contiguous_episode_lengths(self):
        dwell = {d["cluster_id"]: d for d in compute_cluster_dwell_times(self.records)}
        # Cụm 2 chỉ có 1 đợt kéo dài 3 tuần liên tiếp ở AAA
        self.assertEqual(dwell[2]["episode_count"], 1)
        self.assertEqual(dwell[2]["total_weeks"], 3)
        self.assertEqual(dwell[2]["avg_dwell_weeks"], 3.0)
        self.assertEqual(dwell[2]["max_dwell_weeks"], 3)
        self.assertAlmostEqual(dwell[2]["retention_probability"], 2.0 / 3.0, places=6)


class PatientAssociationRuleTests(unittest.TestCase):
    def test_apriori_discovers_high_lift_risk_rule(self):
        patients = []
        # Nhóm nguy cơ cao: 40 người trên 60 tuổi + Tiểu đường + Chưa tiêm -> 36 người CONFIRMED_COVID
        for i in range(40):
            patients.append(
                {
                    "birth_year": 1950,
                    "reported_year": 2021,
                    "gender": "M" if i % 2 == 0 else "F",
                    "comorbidity": "Diabetes",
                    "dose_count": 0,
                    "primary_vaccine": None,
                    "health_status": "CONFIRMED_COVID" if i < 36 else "HEALTHY",
                }
            )
        # Nhóm an toàn: 160 người trẻ 18-39 + Không bệnh nền + Tiêm 3 mũi -> 150 người HEALTHY
        for i in range(160):
            patients.append(
                {
                    "birth_year": 1995,
                    "reported_year": 2021,
                    "gender": "F" if i % 2 == 0 else "M",
                    "comorbidity": "None",
                    "dose_count": 3,
                    "primary_vaccine": "Pfizer-BioNTech",
                    "health_status": "HEALTHY" if i < 150 else "CONFIRMED_COVID",
                }
            )

        rules = mine_patient_association_rules(
            patients,
            min_support=0.05,
            min_confidence=0.60,
            min_lift=1.5,
            max_antecedent_len=3,
        )

        self.assertTrue(len(rules) > 0)
        covid_rules = [r for r in rules if r["target_category_vi"] == "Dương tính COVID-19"]
        self.assertTrue(len(covid_rules) > 0)
        top_covid_rule = covid_rules[0]
        self.assertGreater(top_covid_rule["lift"], 3.0)
        self.assertGreaterEqual(top_covid_rule["confidence"], 0.85)
        self.assertIn("Bệnh nền: Tiểu đường", top_covid_rule["antecedents"])


class AnomalyDetectionTests(unittest.TestCase):
    def test_isolation_forest_flags_extreme_outbreak_spike(self):
        from datetime import date, timedelta

        rows = []
        start = date(2021, 1, 4)
        for i in range(40):
            wk = (start + timedelta(days=7 * i)).isoformat()
            # Tuần 30 có đột biến cực lớn về ca mắc và tử vong
            if i == 30:
                cases, deaths, growth = 2500.0, 45.0, 2400.0
            else:
                cases, deaths, growth = 20.0 + (i % 3), 0.2, 2.0
            rows.append(
                {
                    "week_start": wk,
                    "country_code": "VNM",
                    "country_name": "Vietnam",
                    "cases_per_100k": cases,
                    "deaths_per_100k": deaths,
                    "case_growth_pct": growth,
                }
            )

        anomalies = detect_weekly_anomalies(rows, contamination=0.05)
        self.assertTrue(len(anomalies) >= 1)
        flagged_weeks = [str(a["week_start"]) for a in anomalies]
        expected_spike_week = (start + timedelta(days=7 * 30)).isoformat()
        self.assertIn(expected_spike_week, flagged_weeks)
        self.assertTrue(all(a["anomaly_type_vi"] for a in anomalies))


if __name__ == "__main__":
    unittest.main()
