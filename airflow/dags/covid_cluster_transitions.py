# -*- coding: utf-8 -*-
"""
Khai phá Chuỗi Chuyển dịch Cụm Dịch tễ (Sequential Cluster Transition Mining):
1. Ma trận xác suất chuyển trạng thái Markov bậc 1: P(Cluster_{t+1} = j | Cluster_t = i)
2. Thống kê thời gian lưu trú liên tục (Dwell Time / Episode Length) trong từng cụm.
"""

from collections import defaultdict
from datetime import date
import numpy as np


def _norm_date(val) -> date:
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val)[:10])


def _classify_transition_vi(from_cid: int, to_cid: int) -> str:
    if from_cid == to_cid:
        return "Giữ nguyên mức dịch"
    if to_cid > from_cid:
        return "Leo thang (Dịch nặng lên)"
    return "Hạ nhiệt (Dịch giảm xuống)"


def compute_cluster_transitions(records: list[dict]) -> list[dict]:
    """
    Compute first-order Markov transition matrix across consecutive weekly observations
    of each country.
    """
    if not records:
        raise ValueError("No weekly clustered records provided for transition mining")

    by_country = defaultdict(list)
    cluster_labels = {}
    for rec in records:
        try:
            cid = int(rec["cluster_id"])
            wk = _norm_date(rec["week_start"])
            code = str(rec["country_code"]).strip()
        except (KeyError, TypeError, ValueError):
            continue
        if not code:
            continue
        label = str(rec.get("cluster_label") or f"Cụm {cid}")
        cluster_labels[cid] = label
        by_country[code].append((wk, cid, label))

    if not by_country:
        raise ValueError("No valid country-week cluster sequences found")

    pair_counts = defaultdict(int)
    outgoing_totals = defaultdict(int)

    for code, seq in by_country.items():
        seq_sorted = sorted(seq, key=lambda x: x[0])
        for idx in range(len(seq_sorted) - 1):
            _, from_cid, _ = seq_sorted[idx]
            _, to_cid, _ = seq_sorted[idx + 1]
            pair_counts[(from_cid, to_cid)] += 1
            outgoing_totals[from_cid] += 1

    all_cids = sorted(cluster_labels.keys())
    transitions = []
    for from_cid in all_cids:
        total_out = outgoing_totals.get(from_cid, 0)
        if total_out == 0:
            continue
        from_lbl = cluster_labels[from_cid]
        for to_cid in all_cids:
            cnt = pair_counts.get((from_cid, to_cid), 0)
            to_lbl = cluster_labels[to_cid]
            prob = float(cnt / total_out) if total_out > 0 else 0.0
            transitions.append(
                {
                    "from_cluster_id": int(from_cid),
                    "from_cluster_label": from_lbl,
                    "to_cluster_id": int(to_cid),
                    "to_cluster_label": to_lbl,
                    "transition_count": int(cnt),
                    "from_total_count": int(total_out),
                    "transition_probability": prob,
                    "is_self_transition": bool(from_cid == to_cid),
                    "transition_type_vi": _classify_transition_vi(from_cid, to_cid),
                }
            )

    return transitions


def compute_cluster_dwell_times(records: list[dict]) -> list[dict]:
    """
    Compute contiguous episode lengths (dwell time in weeks) for each cluster
    alongside its self-transition retention probability.
    """
    transitions = compute_cluster_transitions(records)
    retention_by_cid = {
        t["from_cluster_id"]: t["transition_probability"]
        for t in transitions
        if t["is_self_transition"]
    }

    by_country = defaultdict(list)
    cluster_labels = {}
    for rec in records:
        try:
            cid = int(rec["cluster_id"])
            wk = _norm_date(rec["week_start"])
            code = str(rec["country_code"]).strip()
        except (KeyError, TypeError, ValueError):
            continue
        if not code:
            continue
        label = str(rec.get("cluster_label") or f"Cụm {cid}")
        cluster_labels[cid] = label
        by_country[code].append((wk, cid))

    episodes_by_cid = defaultdict(list)
    for _, seq in by_country.items():
        seq_sorted = sorted(seq, key=lambda x: x[0])
        if not seq_sorted:
            continue
        current_cid = seq_sorted[0][1]
        streak = 1
        for _, cid in seq_sorted[1:]:
            if cid == current_cid:
                streak += 1
            else:
                episodes_by_cid[current_cid].append(streak)
                current_cid = cid
                streak = 1
        episodes_by_cid[current_cid].append(streak)

    dwell_rows = []
    for cid in sorted(cluster_labels.keys()):
        streaks = np.asarray(episodes_by_cid.get(cid, [1]), dtype=float)
        dwell_rows.append(
            {
                "cluster_id": int(cid),
                "cluster_label": cluster_labels[cid],
                "episode_count": int(len(streaks)),
                "total_weeks": int(np.sum(streaks)),
                "avg_dwell_weeks": float(np.mean(streaks)),
                "median_dwell_weeks": float(np.median(streaks)),
                "max_dwell_weeks": int(np.max(streaks)),
                "retention_probability": float(retention_by_cid.get(cid, 0.0)),
            }
        )

    return dwell_rows


def run_cluster_transition_mining(records: list[dict]) -> dict:
    return {
        "transitions": compute_cluster_transitions(records),
        "dwell_times": compute_cluster_dwell_times(records),
    }
