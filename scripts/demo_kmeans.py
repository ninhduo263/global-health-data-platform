# -*- coding: utf-8 -*-
"""
Kịch bản Demo Data Mining COVID-19:
1. Đánh giá số cụm K (Elbow, Silhouette, Davies-Bouldin, Calinski-Harabasz) & Hồ sơ cụm K-Means + PCA
2. Cảnh báo sớm bùng phát dịch bằng XGBoost (so sánh 3 mô hình + Feature Importance + Cảnh báo mới nhất)
DỮ LIỆU THẬT 100% TỪ DATA WAREHOUSE (PostgreSQL health_dwh)
"""

import subprocess
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def run_psql(query: str) -> str:
    """Truy vấn trực tiếp PostgreSQL DWH trong container postgres-dwh."""
    cmd = [
        "docker",
        "exec",
        "-i",
        "postgres-dwh",
        "psql",
        "-U",
        "dwh_admin",
        "-d",
        "health_dwh",
        "-c",
        query,
    ]
    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if res.returncode != 0:
        return f"Lỗi SQL: {res.stderr}"
    return res.stdout.strip()


def main():
    print("=" * 92)
    print("   DEMO DATA MINING COVID-19: PHÂN CỤM K-MEANS NÂNG CAO & CẢNH BÁO SỚM BẰNG XGBOOST")
    print("             Nền tảng: Apache Airflow -> PostgreSQL DWH -> Apache Superset")
    print("=" * 92)
    print()

    # BƯỚC 1: ĐÁNH GIÁ CHẤT LƯỢNG CỤM THEO K (K = 2..8)
    print("--- [BƯỚC 1] ĐÁNH GIÁ SỐ CỤM K (dwh.vw_kmeans_evaluation) ---")
    sql_eval = """
    SELECT
        k AS "K",
        ROUND(inertia::numeric, 0) AS "Inertia (Elbow)",
        ROUND(silhouette::numeric, 4) AS "Silhouette",
        ROUND(davies_bouldin::numeric, 4) AS "Davies-Bouldin",
        ROUND(calinski_harabasz::numeric, 1) AS "Calinski-Harabasz",
        is_selected AS "Đang chọn",
        is_best_silhouette AS "Max Sil"
    FROM dwh.vw_kmeans_evaluation
    WHERE model_version = 'weekly-kmeans-v1'
    ORDER BY k;
    """
    print(run_psql(sql_eval))
    print()

    # BƯỚC 2: HỒ SƠ CỤM & NHÃN TỰ SINH (CLUSTER PROFILING)
    print("--- [BƯỚC 2] HỒ SƠ CỤM & NHÃN DỊCH TỄ TỰ SINH (dwh.vw_kmeans_cluster_profile) ---")
    sql_profile = """
    SELECT
        cluster_id AS "Cụm",
        cluster_label AS "Nhãn cụm tự sinh",
        member_count AS "Số tuần-QG",
        ROUND((member_share * 100)::numeric, 1) AS "Tỉ trọng (%)",
        ROUND(avg_cases_per_100k::numeric, 1) AS "Ca TB/100k",
        ROUND(median_cases_per_100k::numeric, 1) AS "Ca TV/100k",
        ROUND(avg_deaths_per_100k::numeric, 2) AS "Tử vong TB/100k",
        ROUND(avg_silhouette::numeric, 3) AS "Silhouette cụm"
    FROM dwh.vw_kmeans_cluster_profile
    WHERE model_version = 'weekly-kmeans-v1'
    ORDER BY cluster_id;
    """
    print(run_psql(sql_profile))
    print()

    # BƯỚC 3: KIỂM CHỨNG LỊCH SỬ VIỆT NAM (KÈM NHÃN CỤM & TỌA ĐỘ PCA)
    print("--- [BƯỚC 3] ĐỐI CHIẾU DỮ LIỆU THẬT CỦA VIỆT NAM QUA CÁC ĐỢT DỊCH ---")
    sql_vnm = """
    SELECT
        week_start AS "Tuần",
        cluster_id AS "Cụm",
        cluster_label AS "Nhãn cụm",
        ROUND(cases_per_100k, 1) AS "Ca/100k",
        ROUND(deaths_per_100k, 2) AS "TV/100k",
        ROUND(case_growth_pct, 1) AS "% Tăng",
        ROUND(pca_1::numeric, 2) AS "PCA 1",
        ROUND(pca_2::numeric, 2) AS "PCA 2"
    FROM dwh.vw_weekly_covid_clusters
    WHERE country_code = 'VNM'
      AND week_start IN ('2020-04-06', '2021-05-17', '2021-08-09', '2021-08-30', '2022-03-07', '2022-10-10')
    ORDER BY week_start;
    """
    print(run_psql(sql_vnm))
    print()

    # BƯỚC 4: SO SÁNH 3 MÔ HÌNH CẢNH BÁO SỚM (PURGED TIME SPLIT)
    print("--- [BƯỚC 4] ĐÁNH GIÁ MÔ HÌNH CẢNH BÁO SỚM BÙNG PHÁT DỊCH (dwh.vw_early_warning_evaluation) ---")
    sql_ew_eval = """
    SELECT
        model_version AS "Phiên bản",
        model_name AS "Mô hình",
        is_primary AS "Chính",
        ROUND(decision_threshold::numeric, 2) AS "Ngưỡng",
        ROUND(precision_score::numeric, 3) AS "Precision",
        ROUND(recall_score::numeric, 3) AS "Recall",
        ROUND(f1_score::numeric, 3) AS "F1",
        ROUND(roc_auc::numeric, 3) AS "ROC-AUC",
        ROUND(pr_auc::numeric, 3) AS "PR-AUC",
        train_rows AS "Train",
        test_rows AS "Test"
    FROM dwh.vw_early_warning_evaluation
    ORDER BY model_version, is_primary DESC, model_name;
    """
    print(run_psql(sql_ew_eval))
    print()

    # BƯỚC 5: TOP 8 ĐẶC TRƯNG QUAN TRỌNG NHẤT CỦA MÔ HÌNH CHÍNH
    print("--- [BƯỚC 5] TOP 8 ĐẶC TRƯNG QUAN TRỌNG NHẤT (dwh.vw_early_warning_feature_importance) ---")
    sql_imp = """
    SELECT
        importance_rank AS "Hạng",
        feature_name AS "Đặc trưng",
        ROUND((importance * 100)::numeric, 2) AS "Độ quan trọng (%)"
    FROM dwh.vw_early_warning_feature_importance
    WHERE model_version = 'early-warning-v1'
    ORDER BY importance_rank
    LIMIT 8;
    """
    print(run_psql(sql_imp))
    print()

    # BƯỚC 6: TOP 10 QUỐC GIA CÓ XÁC SUẤT CẢNH BÁO CAO NHẤT TRONG KỲ DỰ BÁO MỚI NHẤT
    print("--- [BƯỚC 6] CẢNH BÁO MỚI NHẤT THEO QUỐC GIA (dwh.vw_early_warning_latest) ---")
    sql_latest = """
    SELECT
        week_start AS "Tuần dự báo",
        country_code AS "Mã QG",
        country_name AS "Quốc gia",
        ROUND(cases_per_100k::numeric, 1) AS "Ca/100k hiện tại",
        ROUND((surge_probability * 100)::numeric, 1) AS "Xác suất bùng phát (%)",
        alert_level AS "Mức cảnh báo"
    FROM dwh.vw_early_warning_latest
    WHERE model_version = 'early-warning-v1'
      AND cases_per_100k > 0
    ORDER BY surge_probability DESC, cases_per_100k DESC
    LIMIT 10;
    """
    print(run_psql(sql_latest))
    print()

    # BƯỚC 7: MA TRẬN CHUYỂN DỊCH CỤM MARKOV & THỜI GIAN LƯU TRÚ (DWELL TIME)
    print("--- [BƯỚC 7] KHAI PHÁ CHUỖI CHUYỂN DỊCH CỤM MARKOV & DWELL TIME (dwh.vw_cluster_*) ---")
    sql_dwell = """
    SELECT
        cluster_id AS "Cụm",
        cluster_label AS "Nhãn cụm",
        episode_count AS "Số đợt",
        ROUND(avg_dwell_weeks::numeric, 2) AS "Lưu trú TB (tuần)",
        max_dwell_weeks AS "Đợt dài nhất (tuần)",
        retention_prob_pct AS "Xác suất giữ cụm (%)"
    FROM dwh.vw_cluster_dwell_time
    WHERE model_version = 'weekly-kmeans-v1'
    ORDER BY cluster_id;
    """
    print(run_psql(sql_dwell))
    print()

    sql_trans = """
    SELECT
        from_cluster_id AS "Từ",
        to_cluster_id AS "Sang",
        transition_pair_vi AS "Chiều chuyển dịch cụm",
        transition_count AS "Số lần",
        transition_prob_pct AS "Xác suất (%)",
        transition_type_vi AS "Xu hướng"
    FROM dwh.vw_cluster_transition
    WHERE model_version = 'weekly-kmeans-v1'
      AND transition_count > 0
    ORDER BY from_cluster_id, to_cluster_id;
    """
    print(run_psql(sql_trans))
    print()

    # BƯỚC 8: KHAI PHÁ LUẬT KẾT HỢP BỆNH NHÂN & TIÊM CHỦNG (APRIORI / FP-GROWTH)
    print("--- [BƯỚC 8] TOP 10 LUẬT KẾT HỢP BỆNH NHÂN & TIÊM CHỦNG (dwh.vw_patient_association_rules) ---")
    sql_rules = """
    SELECT
        rule_id AS "Hạng",
        antecedents AS "Tổ hợp điều kiện (Vế trái)",
        target_category_vi AS "Kết cục (Vế phải)",
        support_count AS "Số BN",
        support_pct AS "Support (%)",
        confidence_pct AS "Confidence (%)",
        ROUND(lift::numeric, 2) AS "Lift"
    FROM dwh.vw_patient_association_rules
    WHERE model_version = 'apriori-v1'
    ORDER BY rule_id
    LIMIT 10;
    """
    print(run_psql(sql_rules))
    print()
    print("=" * 92)


if __name__ == "__main__":
    main()
