import math
from typing import Iterable, Union

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_samples,
    silhouette_score,
)
from sklearn.preprocessing import StandardScaler

SEVERITY_LABELS_BY_K = {
    1: ("Thấp",),
    2: ("Thấp", "Cao"),
    3: ("Thấp", "Trung bình", "Cao"),
    4: ("Thấp", "Trung bình", "Cao", "Rất cao"),
    5: ("Rất thấp", "Thấp", "Trung bình", "Cao", "Rất cao"),
    6: ("Rất thấp", "Thấp", "Trung bình thấp", "Trung bình cao", "Cao", "Rất cao"),
    7: (
        "Rất thấp",
        "Thấp",
        "Trung bình thấp",
        "Trung bình",
        "Trung bình cao",
        "Cao",
        "Rất cao",
    ),
    8: (
        "Rất thấp",
        "Thấp",
        "Khá thấp",
        "Trung bình thấp",
        "Trung bình cao",
        "Khá cao",
        "Cao",
        "Rất cao",
    ),
}


def parse_k_range(k_range: Union[str, Iterable[int]] = "2-8") -> list[int]:
    """Parse K range specification such as '2-8' or '2,3,4,5' into sorted ints >= 2."""
    if isinstance(k_range, str):
        spec = k_range.strip()
        if not spec:
            raise ValueError("k_range cannot be empty")
        values = set()
        for part in spec.split(","):
            token = part.strip()
            if not token:
                continue
            if "-" in token:
                bounds = token.split("-", 1)
                start, end = int(bounds[0].strip()), int(bounds[1].strip())
                if start > end:
                    start, end = end, start
                values.update(range(start, end + 1))
            else:
                values.add(int(token))
    else:
        values = {int(v) for v in k_range}

    filtered = sorted(k for k in values if k >= 2)
    if not filtered:
        raise ValueError("k_range must include at least one K >= 2")
    return filtered


def _validate_requested_clusters(requested_clusters: Union[int, str]) -> Union[int, str]:
    if isinstance(requested_clusters, str) and requested_clusters.strip().lower() == "auto":
        return "auto"
    try:
        parsed = int(requested_clusters)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "requested_clusters must be a positive integer or 'auto'"
        ) from exc
    if parsed < 1:
        raise ValueError("requested_clusters must be at least 1")
    return parsed


def prepare_weekly_features(records) -> tuple[list[dict], np.ndarray, StandardScaler]:
    """Normalize weekly incidence/mortality rates and standardize log features."""
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

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(np.asarray(feature_rows, dtype=float))
    return enriched_records, scaled_features, scaler


def evaluate_cluster_counts(
    X_scaled: np.ndarray,
    k_values: Iterable[int] = range(2, 9),
    sample_size: int = 10_000,
    random_state: int = 42,
) -> list[dict]:
    """Evaluate candidate K values using Inertia, Silhouette, Davies-Bouldin, and Calinski-Harabasz."""
    n_samples = int(X_scaled.shape[0])
    distinct_rows = int(np.unique(X_scaled, axis=0).shape[0])
    sorted_ks = sorted({int(k) for k in k_values if int(k) >= 2})

    evaluations = []
    for k in sorted_ks:
        if k > distinct_rows:
            continue

        model = KMeans(n_clusters=k, random_state=random_state, n_init=10)
        labels = model.fit_predict(X_scaled)
        inertia = float(model.inertia_)

        unique_labels = int(np.unique(labels).shape[0])
        silhouette = None
        davies_bouldin = None
        calinski_harabasz = None
        sil_rows = 0

        if 2 <= unique_labels <= n_samples - 1:
            use_sample = (
                int(sample_size)
                if sample_size is not None and n_samples > int(sample_size)
                else None
            )
            sil_rows = int(use_sample if use_sample is not None else n_samples)
            try:
                silhouette = float(
                    silhouette_score(
                        X_scaled,
                        labels,
                        sample_size=use_sample,
                        random_state=random_state,
                    )
                )
            except ValueError:
                silhouette = None
                sil_rows = 0
            davies_bouldin = float(davies_bouldin_score(X_scaled, labels))
            calinski_harabasz = float(calinski_harabasz_score(X_scaled, labels))

        evaluations.append(
            {
                "k": k,
                "inertia": inertia,
                "silhouette": silhouette,
                "davies_bouldin": davies_bouldin,
                "calinski_harabasz": calinski_harabasz,
                "total_rows": n_samples,
                "silhouette_sample_rows": sil_rows,
                "is_selected": False,
                "is_best_silhouette": False,
            }
        )

    valid_sil = [row for row in evaluations if row["silhouette"] is not None]
    if valid_sil:
        best_row = max(valid_sil, key=lambda row: (row["silhouette"], -row["k"]))
        best_row["is_best_silhouette"] = True

    return evaluations


def select_cluster_count(
    evaluations: list[dict],
    requested_clusters: Union[int, str] = 4,
    distinct_rows: int = None,
) -> int:
    """Select final K (fixed integer or 'auto' by highest Silhouette) and flag is_selected."""
    validated = _validate_requested_clusters(requested_clusters)

    if validated == "auto":
        best_rows = [row for row in evaluations if row.get("is_best_silhouette")]
        if best_rows:
            selected_k = int(best_rows[0]["k"])
        elif evaluations:
            valid_sil = [row for row in evaluations if row.get("silhouette") is not None]
            if valid_sil:
                selected_k = int(
                    max(valid_sil, key=lambda r: (r["silhouette"], -r["k"]))["k"]
                )
            else:
                selected_k = int(evaluations[0]["k"])
        else:
            selected_k = 1 if (distinct_rows is None or distinct_rows <= 1) else 2
    else:
        if distinct_rows is not None:
            selected_k = min(validated, int(distinct_rows))
        elif evaluations:
            selected_k = min(validated, max(int(row["k"]) for row in evaluations))
        else:
            selected_k = validated

    for row in evaluations:
        row["is_selected"] = int(row["k"]) == selected_k

    return selected_k


def _severity_label(cluster_id: int, cluster_count: int) -> str:
    if cluster_count in SEVERITY_LABELS_BY_K:
        return SEVERITY_LABELS_BY_K[cluster_count][cluster_id]
    base = SEVERITY_LABELS_BY_K[8]
    mapped_idx = min(
        len(base) - 1,
        int(round(cluster_id * (len(base) - 1) / max(1, cluster_count - 1))),
    )
    return f"{base[mapped_idx]} (C{cluster_id})"


def _trend_label(centroid_log_change: float) -> str:
    if centroid_log_change > 0.15:
        return "đang tăng"
    if centroid_log_change < -0.15:
        return "đang giảm"
    return "ổn định"


def build_cluster_profiles(
    records: list[dict],
    ordered_labels: np.ndarray,
    X_scaled: np.ndarray,
    scaler: StandardScaler,
    ordered_centroids_scaled: np.ndarray,
    sample_size: int = 10_000,
    random_state: int = 42,
) -> list[dict]:
    """Build business-interpretable cluster profiles in original epidemiological units."""
    total_count = len(records)
    cluster_count = int(ordered_centroids_scaled.shape[0])
    centroids_log = scaler.inverse_transform(ordered_centroids_scaled)

    per_cluster_silhouette = {}
    unique_all = int(np.unique(ordered_labels).shape[0])
    if 2 <= unique_all <= total_count - 1:
        if sample_size is not None and total_count > int(sample_size):
            rng = np.random.RandomState(random_state)
            sample_idx = rng.choice(total_count, size=int(sample_size), replace=False)
            X_sub = X_scaled[sample_idx]
            labels_sub = ordered_labels[sample_idx]
        else:
            X_sub = X_scaled
            labels_sub = ordered_labels

        if 2 <= int(np.unique(labels_sub).shape[0]) <= len(labels_sub) - 1:
            sample_sil_values = silhouette_samples(X_sub, labels_sub)
            for cid in range(cluster_count):
                mask = labels_sub == cid
                if np.any(mask):
                    per_cluster_silhouette[cid] = float(np.mean(sample_sil_values[mask]))

    profiles = []
    for cid in range(cluster_count):
        members = [rec for rec, lbl in zip(records, ordered_labels) if int(lbl) == cid]
        member_count = len(members)
        cases_arr = np.asarray([m["cases_per_100k"] for m in members], dtype=float)
        deaths_arr = np.asarray([m["deaths_per_100k"] for m in members], dtype=float)
        growth_vals = [
            float(m["case_growth_pct"])
            for m in members
            if m.get("case_growth_pct") is not None
        ]

        c_log_cases, c_log_deaths, c_log_change = (
            float(centroids_log[cid, 0]),
            float(centroids_log[cid, 1]),
            float(centroids_log[cid, 2]),
        )
        label_text = f"{_severity_label(cid, cluster_count)} – {_trend_label(c_log_change)}"

        profiles.append(
            {
                "cluster_id": cid,
                "cluster_label": label_text,
                "member_count": member_count,
                "member_share": float(member_count / total_count) if total_count else 0.0,
                "avg_cases_per_100k": float(np.mean(cases_arr)) if member_count else 0.0,
                "median_cases_per_100k": float(np.median(cases_arr)) if member_count else 0.0,
                "avg_deaths_per_100k": float(np.mean(deaths_arr)) if member_count else 0.0,
                "median_case_growth_pct": (
                    float(np.median(growth_vals)) if growth_vals else None
                ),
                "centroid_log_cases": c_log_cases,
                "centroid_log_deaths": c_log_deaths,
                "centroid_log_change": c_log_change,
                "avg_silhouette": per_cluster_silhouette.get(cid),
            }
        )

    return profiles


def _compute_pca_2d(X_scaled: np.ndarray, random_state: int = 42) -> np.ndarray:
    n_samples, n_features = X_scaled.shape
    coords = np.zeros((n_samples, 2), dtype=float)
    n_comp = min(2, n_samples, n_features)
    if n_comp >= 1 and np.any(np.var(X_scaled, axis=0) > 0):
        projected = PCA(n_components=n_comp, random_state=random_state).fit_transform(
            X_scaled
        )
        coords[:, :n_comp] = projected
    return coords


def run_weekly_clustering(
    records,
    requested_clusters: Union[int, str] = 4,
    k_range: Union[str, Iterable[int]] = "2-8",
    sample_size: int = 10_000,
    random_state: int = 42,
) -> dict:
    """Run full K-means evaluation, selection, cluster profiling, and 2D PCA projection."""
    validated_req = _validate_requested_clusters(requested_clusters)
    enriched_records, X_scaled, scaler = prepare_weekly_features(records)
    distinct_rows = int(np.unique(X_scaled, axis=0).shape[0])

    candidate_ks = parse_k_range(k_range)
    if isinstance(validated_req, int) and validated_req >= 2 and validated_req not in candidate_ks:
        candidate_ks = sorted(set(candidate_ks) | {validated_req})

    evaluations = evaluate_cluster_counts(
        X_scaled,
        k_values=candidate_ks,
        sample_size=sample_size,
        random_state=random_state,
    )
    cluster_count = select_cluster_count(
        evaluations,
        requested_clusters=validated_req,
        distinct_rows=distinct_rows,
    )

    if cluster_count == 1:
        raw_labels = np.zeros(len(enriched_records), dtype=int)
        raw_centroids = np.mean(X_scaled, axis=0, keepdims=True)
    else:
        final_model = KMeans(
            n_clusters=cluster_count,
            random_state=random_state,
            n_init=10,
        )
        raw_labels = final_model.fit_predict(X_scaled)
        raw_centroids = final_model.cluster_centers_

    cluster_order = sorted(
        range(cluster_count),
        key=lambda cid: np.mean(
            [
                rec["cases_per_100k"]
                for rec, lbl in zip(enriched_records, raw_labels)
                if int(lbl) == cid
            ]
        ),
    )
    ordered_map = {orig_id: new_id for new_id, orig_id in enumerate(cluster_order)}
    ordered_labels = np.asarray([ordered_map[int(lbl)] for lbl in raw_labels], dtype=int)
    ordered_centroids = np.asarray([raw_centroids[orig_id] for orig_id in cluster_order], dtype=float)

    profiles = build_cluster_profiles(
        enriched_records,
        ordered_labels,
        X_scaled,
        scaler,
        ordered_centroids,
        sample_size=sample_size,
        random_state=random_state,
    )
    label_by_cluster = {p["cluster_id"]: p["cluster_label"] for p in profiles}
    pca_coords = _compute_pca_2d(X_scaled, random_state=random_state)

    for idx, (record, cid) in enumerate(zip(enriched_records, ordered_labels)):
        cid_int = int(cid)
        record["cluster_id"] = cid_int
        record["cluster_label"] = label_by_cluster[cid_int]
        record["pca_1"] = float(pca_coords[idx, 0])
        record["pca_2"] = float(pca_coords[idx, 1])

    return {
        "records": enriched_records,
        "evaluations": evaluations,
        "profiles": profiles,
        "cluster_count": cluster_count,
    }


def cluster_weekly_records(records, requested_clusters: Union[int, str] = 4) -> list[dict]:
    """Backward-compatible helper returning enriched weekly records with cluster assignments."""
    return run_weekly_clustering(records, requested_clusters=requested_clusters)["records"]
