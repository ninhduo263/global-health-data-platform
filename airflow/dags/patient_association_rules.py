# -*- coding: utf-8 -*-
"""
Khai phá Luật Kết hợp (Association Rule Mining — Apriori / FP-Growth Lattice)
trên dữ liệu Hồ sơ Bệnh nhân ẩn danh (dwh.dim_patient) kết hợp Lịch sử Tiêm chủng (dwh.fact_vaccination).

Mục tiêu:
- Tìm các tổ hợp nhân khẩu học, bệnh nền và trạng thái tiêm chủng dẫn tới kết cục sức khỏe
  (Dương tính COVID-19, Nghi nhiễm, Đã khỏi bệnh, Bình thường).
- Tính chính xác Support, Confidence, Lift, Leverage, Conviction hoàn toàn bằng Python chuẩn.
"""

from collections import Counter
from itertools import combinations


def _age_bucket_vi(birth_year, reference_year: int = 2022) -> str:
    try:
        by = int(birth_year)
    except (TypeError, ValueError):
        return "Tuổi: Chưa rõ"
    age = max(0, int(reference_year) - by)
    if age < 18:
        return "Tuổi: Dưới 18"
    if age < 40:
        return "Tuổi: 18 – 39"
    if age < 60:
        return "Tuổi: 40 – 59"
    return "Tuổi: Từ 60 trở lên"


def _gender_item_vi(gender: str) -> str:
    g = (gender or "").strip().upper()
    if g == "M":
        return "Giới tính: Nam"
    if g == "F":
        return "Giới tính: Nữ"
    return "Giới tính: Khác"


def _comorbidity_item_vi(comorbidity: str) -> str:
    c = (comorbidity or "None").strip()
    mapping = {
        "None": "Bệnh nền: Không có",
        "Hypertension": "Bệnh nền: Tăng huyết áp",
        "Diabetes": "Bệnh nền: Tiểu đường",
        "Cardiovascular": "Bệnh nền: Tim mạch",
        "Chronic Respiratory": "Bệnh nền: Hô hấp mạn tính",
        "Obesity": "Bệnh nền: Béo phì",
    }
    return mapping.get(c, f"Bệnh nền: {c}")


def _dose_item_vi(dose_count: int) -> str:
    d = int(dose_count or 0)
    if d <= 0:
        return "Tiêm chủng: Chưa tiêm (0 mũi)"
    if d == 1:
        return "Tiêm chủng: Mới tiêm 1 mũi"
    if d == 2:
        return "Tiêm chủng: Đã tiêm đủ 2 mũi"
    return "Tiêm chủng: Đã tiêm nhắc lại (≥ 3 mũi)"


def _health_status_item_vi(status: str) -> tuple[str, str]:
    s = (status or "HEALTHY").strip().upper()
    mapping = {
        "CONFIRMED_COVID": ("Kết quả: Dương tính COVID-19", "Dương tính COVID-19"),
        "SUSPECTED": ("Kết quả: Nghi nhiễm", "Nghi nhiễm"),
        "RECOVERED": ("Kết quả: Đã khỏi bệnh", "Đã khỏi bệnh"),
        "HEALTHY": ("Kết quả: Bình thường", "Bình thường"),
    }
    return mapping.get(s, (f"Kết quả: {s}", s))


def build_patient_transactions(patient_records: list[dict]) -> list[dict]:
    """
    Transform raw patient + vaccination summary rows into discrete itemset transactions.
    Each transaction has `antecedent_items` (set of str) and `consequent_item` / `target_category_vi`.
    """
    transactions = []
    for row in patient_records:
        ref_year = int(row.get("reported_year") or 2022)
        items = {
            _age_bucket_vi(row.get("birth_year"), ref_year),
            _gender_item_vi(row.get("gender")),
            _comorbidity_item_vi(row.get("comorbidity")),
            _dose_item_vi(row.get("dose_count", 0)),
        }
        primary_vax = (row.get("primary_vaccine") or "").strip()
        if primary_vax and int(row.get("dose_count") or 0) > 0:
            items.add(f"Vắc-xin: {primary_vax}")

        consequent_item, target_vi = _health_status_item_vi(row.get("health_status"))
        transactions.append(
            {
                "antecedent_items": frozenset(items),
                "consequent_item": consequent_item,
                "target_category_vi": target_vi,
            }
        )

    if not transactions:
        raise ValueError("No patient transactions available for association rule mining")
    return transactions


def mine_patient_association_rules(
    patient_records: list[dict],
    min_support: float = 0.015,
    min_confidence: float = 0.20,
    min_lift: float = 1.05,
    max_antecedent_len: int = 3,
    top_k: int = 40,
) -> list[dict]:
    """
    Mine association rules of the form {Demographic / Comorbidity / Vaccination} => {Health Status}.
    Sorted by Lift descending, then Confidence descending.
    """
    if min_support <= 0 or min_support > 1:
        raise ValueError("min_support must be in (0, 1]")
    if min_confidence <= 0 or min_confidence > 1:
        raise ValueError("min_confidence must be in (0, 1]")

    transactions = build_patient_transactions(patient_records)
    n_trans = len(transactions)

    antecedent_counts = Counter()
    consequent_counts = Counter()
    joint_counts = Counter()
    target_vi_by_consequent = {}

    for tx in transactions:
        ant_sorted = tuple(sorted(tx["antecedent_items"]))
        cons = tx["consequent_item"]
        target_vi_by_consequent[cons] = tx["target_category_vi"]
        consequent_counts[cons] += 1

        max_r = min(int(max_antecedent_len), len(ant_sorted))
        for r in range(1, max_r + 1):
            for comb in combinations(ant_sorted, r):
                antecedent_counts[comb] += 1
                joint_counts[(comb, cons)] += 1

    rules = []
    for (ant_tuple, cons), joint_cnt in joint_counts.items():
        support = joint_cnt / n_trans
        if support < min_support:
            continue

        ant_cnt = antecedent_counts[ant_tuple]
        cons_cnt = consequent_counts[cons]
        sup_ant = ant_cnt / n_trans
        sup_cons = cons_cnt / n_trans

        confidence = joint_cnt / ant_cnt if ant_cnt > 0 else 0.0
        if confidence < min_confidence:
            continue

        lift = confidence / sup_cons if sup_cons > 0 else 0.0
        if lift < min_lift:
            continue

        leverage = support - (sup_ant * sup_cons)
        if confidence >= 1.0 - 1e-9:
            conviction = 99.99
        else:
            conviction = min(99.99, (1.0 - sup_cons) / max(1e-9, 1.0 - confidence))

        ant_str = " + ".join(ant_tuple)
        rules.append(
            {
                "antecedents": ant_str,
                "consequent": cons,
                "rule_text_vi": f"Nếu [{ant_str}] ⇒ [{cons}]",
                "antecedent_count": len(ant_tuple),
                "support_count": int(joint_cnt),
                "total_transactions": int(n_trans),
                "support": float(support),
                "confidence": float(confidence),
                "lift": float(lift),
                "leverage": float(leverage),
                "conviction": float(conviction),
                "target_category_vi": target_vi_by_consequent[cons],
            }
        )

    rules.sort(key=lambda r: (-r["lift"], -r["confidence"], -r["support"], r["antecedents"]))
    if top_k is not None and top_k > 0:
        rules = rules[: int(top_k)]

    for idx, rule in enumerate(rules, start=1):
        rule["rule_id"] = idx

    return rules
