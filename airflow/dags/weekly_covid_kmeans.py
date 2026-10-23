import math

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


def cluster_weekly_records(records, requested_clusters=4):
    if requested_clusters < 1:
        raise ValueError("requested_clusters must be at least 1")

    enriched_records = []
    feature_rows = []

    for record in records:
        try:
            population = float(record["population"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(population) or population <= 0:
            continue

        def nonnegative_number(field):
            try:
                value = float(record.get(field) or 0)
            except (TypeError, ValueError):
                return 0.0
            return value if math.isfinite(value) and value >= 0 else 0.0

        weekly_cases = nonnegative_number("weekly_cases")
        weekly_deaths = nonnegative_number("weekly_deaths")
        cases_per_100k = weekly_cases * 100_000 / population
        deaths_per_100k = weekly_deaths * 100_000 / population
        previous_cases = record.get("previous_week_cases")
        previous_rate = (
            cases_per_100k
            if previous_cases is None
            else nonnegative_number("previous_week_cases") * 100_000 / population
        )
        log_case_rate_change = math.log1p(cases_per_100k) - math.log1p(previous_rate)
        case_growth_pct = (
            (cases_per_100k - previous_rate) / previous_rate * 100
            if previous_rate > 0 and previous_cases is not None
            else None
        )

        enriched_records.append(
            {
                **record,
                "weekly_cases": int(weekly_cases),
                "weekly_deaths": int(weekly_deaths),
                "population": int(population),
                "cases_per_100k": cases_per_100k,
                "deaths_per_100k": deaths_per_100k,
                "case_growth_pct": case_growth_pct,
            }
        )
        feature_rows.append(
            [
                math.log1p(cases_per_100k),
                math.log1p(deaths_per_100k),
                log_case_rate_change,
            ]
        )

    if not enriched_records:
        raise ValueError("No weekly records with a valid population were found")

    scaled_features = StandardScaler().fit_transform(np.asarray(feature_rows))
    distinct_rows = np.unique(scaled_features, axis=0).shape[0]
    cluster_count = min(requested_clusters, distinct_rows)
    if cluster_count == 1:
        labels = np.zeros(len(enriched_records), dtype=int)
    else:
        labels = KMeans(
            n_clusters=cluster_count,
            random_state=42,
            n_init=10,
        ).fit_predict(scaled_features)

    cluster_order = sorted(
        range(cluster_count),
        key=lambda cluster_id: np.mean(
            [
                record["cases_per_100k"]
                for record, label in zip(enriched_records, labels)
                if label == cluster_id
            ]
        ),
    )
    ordered_cluster_ids = {
        original_id: ordered_id for ordered_id, original_id in enumerate(cluster_order)
    }
    for record, label in zip(enriched_records, labels):
        record["cluster_id"] = ordered_cluster_ids[int(label)]

    return enriched_records