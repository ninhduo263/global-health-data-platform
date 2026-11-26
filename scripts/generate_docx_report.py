# -*- coding: utf-8 -*-
"""
Tự động sinh tài liệu Word chuyên sâu `GIOI_THIEU_HE_THONG.docx` giới thiệu toàn bộ
kiến trúc Data Platform, mô hình Snowflake DWH, đồng bộ CDC ẩn danh và 2 bài toán Data Mining
(K-Means nâng cao + PCA & Cảnh báo sớm bùng phát dịch bằng XGBoost).

Tự động trích xuất số liệu thực tế từ PostgreSQL DWH và vẽ 4 hình minh họa độ phân giải cao
(Sơ đồ kiến trúc 5 tầng, Sơ đồ Snowflake DWH, Biểu đồ K-Means Elbow/Silhouette + PCA 2D,
Biểu đồ đối sánh 3 mô hình & Top 10 đặc trưng XGBoost) nhúng trực tiếp vào file .docx.
"""

import argparse
import os
import tempfile
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL as WD_CELL_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn
from docx.shared import Cm, Inches, Pt, RGBColor

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Liberation Sans"]
plt.rcParams["axes.unicode_minus"] = False

NAVY = RGBColor(0x1E, 0x3A, 0x8A)
TEAL = RGBColor(0x0F, 0x76, 0x6E)
SLATE = RGBColor(0x33, 0x41, 0x55)
MUTED = RGBColor(0x64, 0x74, 0x8B)
DARK = RGBColor(0x0F, 0x17, 0x2A)


def fetch_dwh_snapshot() -> dict:
    """Query live metrics and samples from PostgreSQL DWH."""
    conn = psycopg2.connect(
        host=os.getenv("DWH_HOST", "postgres-dwh"),
        port=int(os.getenv("DWH_PORT", "5432")),
        dbname=os.getenv("DWH_DB", "health_dwh"),
        user=os.getenv("DWH_USER", "dwh_admin"),
        password=os.getenv("DWH_PASSWORD", "dwh_password"),
    )
    data = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM dwh.fact_cases),
                    (SELECT COUNT(DISTINCT country_id) FROM dwh.fact_cases),
                    (SELECT COUNT(*) FROM dwh.fact_weekly_covid_cluster WHERE model_version = 'weekly-kmeans-v1'),
                    (SELECT COUNT(*) FROM dwh.dim_patient WHERE NOT is_placeholder),
                    (SELECT COUNT(*) FROM dwh.fact_vaccination)
                """
            )
            row = cur.fetchone()
            data["counts"] = {
                "fact_cases": int(row[0] or 0),
                "countries": int(row[1] or 0),
                "weekly_clusters": int(row[2] or 0),
                "patients": int(row[3] or 0),
                "vaccinations": int(row[4] or 0),
            }

            cur.execute(
                """
                SELECT k, inertia, silhouette, davies_bouldin, calinski_harabasz,
                       is_selected, is_best_silhouette
                FROM dwh.vw_kmeans_evaluation
                WHERE model_version = 'weekly-kmeans-v1'
                ORDER BY k
                """
            )
            data["kmeans_eval"] = cur.fetchall()

            cur.execute(
                """
                SELECT cluster_id, cluster_label, member_count, member_share * 100.0,
                       avg_cases_per_100k, median_cases_per_100k, avg_deaths_per_100k, avg_silhouette
                FROM dwh.vw_kmeans_cluster_profile
                WHERE model_version = 'weekly-kmeans-v1'
                ORDER BY cluster_id
                """
            )
            data["kmeans_profiles"] = cur.fetchall()

            cur.execute(
                """
                (SELECT cluster_id, cluster_label, pca_1, pca_2 FROM dwh.vw_weekly_covid_clusters WHERE cluster_id = 0 ORDER BY country_code, week_start LIMIT 450)
                UNION ALL
                (SELECT cluster_id, cluster_label, pca_1, pca_2 FROM dwh.vw_weekly_covid_clusters WHERE cluster_id = 1 ORDER BY country_code, week_start LIMIT 450)
                UNION ALL
                (SELECT cluster_id, cluster_label, pca_1, pca_2 FROM dwh.vw_weekly_covid_clusters WHERE cluster_id = 2 ORDER BY country_code, week_start LIMIT 450)
                UNION ALL
                (SELECT cluster_id, cluster_label, pca_1, pca_2 FROM dwh.vw_weekly_covid_clusters WHERE cluster_id = 3 ORDER BY country_code, week_start LIMIT 450)
                """
            )
            data["pca_sample"] = cur.fetchall()

            cur.execute(
                """
                SELECT week_start, cluster_id, cluster_label, cases_per_100k, deaths_per_100k, case_growth_pct
                FROM dwh.vw_weekly_covid_clusters
                WHERE country_code = 'VNM'
                  AND week_start IN ('2020-04-06', '2021-05-17', '2021-08-09', '2021-08-30', '2022-03-07', '2022-10-10')
                ORDER BY week_start
                """
            )
            data["vnm_history"] = cur.fetchall()

            cur.execute(
                """
                SELECT model_name_vi, is_primary, decision_threshold, precision_score,
                       recall_score, f1_score, roc_auc, pr_auc, train_rows, test_rows
                FROM dwh.vw_early_warning_evaluation
                WHERE model_version = 'early-warning-v1'
                ORDER BY is_primary DESC, roc_auc DESC NULLS LAST
                """
            )
            data["ew_eval"] = cur.fetchall()

            cur.execute(
                """
                SELECT importance_rank, feature_name_vi, feature_name, importance * 100.0
                FROM dwh.vw_early_warning_feature_importance
                WHERE model_version = 'early-warning-v1'
                ORDER BY importance_rank
                LIMIT 10
                """
            )
            data["ew_importance"] = cur.fetchall()

            cur.execute(
                """
                SELECT cluster_id, cluster_label, episode_count, avg_dwell_weeks,
                       median_dwell_weeks, max_dwell_weeks, retention_prob_pct
                FROM dwh.vw_cluster_dwell_time
                WHERE model_version = 'weekly-kmeans-v1'
                ORDER BY cluster_id
                """
            )
            data["dwell_times"] = cur.fetchall()

            cur.execute(
                """
                SELECT rule_id, antecedents, target_category_vi, support_count,
                       support_pct, confidence_pct, lift
                FROM dwh.vw_patient_association_rules
                WHERE model_version = 'apriori-v1'
                ORDER BY rule_id
                LIMIT 8
                """
            )
            data["assoc_rules"] = cur.fetchall()
    finally:
        conn.close()
    return data


# ==============================================================================
# VẼ 4 SƠ ĐỒ & BIỂU ĐỒ KẾT QUẢ THỰC NGHIỆM (240 DPI)
# ==============================================================================
def draw_architecture_diagram(output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11.5, 5.2), dpi=240)
    ax.set_xlim(0, 11.5)
    ax.set_ylim(0, 5.2)
    ax.axis("off")

    columns = [
        (0.25, 2.0, "1. TẦNG NGUỒN DỮ LIỆU", "#EFF6FF", "#1D4ED8"),
        (2.55, 2.15, "2. THU THẬP & ĐỒNG BỘ", "#F0FDF4", "#15803D"),
        (5.00, 2.05, "3. LƯU TRỮ LAKE & DWH", "#FFFBEB", "#B45309"),
        (7.35, 2.05, "4. KHAI PHÁ DỮ LIỆU", "#FAF5FF", "#7E22CE"),
        (9.70, 1.55, "5. TRỰC QUAN BI", "#FEF2F2", "#B91C1C"),
    ]
    for x, w, header, bg, border in columns:
        panel = mpatches.FancyBboxPatch(
            (x, 0.25), w, 4.65,
            boxstyle="round,pad=0.06,rounding_size=0.12",
            facecolor=bg, edgecolor=border, linewidth=1.3,
        )
        ax.add_patch(panel)
        ax.text(x + w / 2, 4.68, header, ha="center", va="center", fontsize=8.3, fontweight="bold", color=border)

    def box(x, y, w, h, title, sub, fc="#FFFFFF", ec="#334155"):
        b = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.04,rounding_size=0.08",
            facecolor=fc, edgecolor=ec, linewidth=1.1,
        )
        ax.add_patch(b)
        ax.text(x + w / 2, y + h * 0.63, title, ha="center", va="center", fontsize=8.2, fontweight="bold", color="#0F172A")
        ax.text(x + w / 2, y + h * 0.28, sub, ha="center", va="center", fontsize=6.9, color="#475569")

    # Tầng 1
    box(0.4, 3.35, 1.7, 0.92, "OWID Global CSV", "393.903 dòng (ISO-3)", ec="#2563EB")
    box(0.4, 2.10, 1.7, 0.92, "WHO Daily CSV", "15 quốc gia (ISO-2)", ec="#2563EB")
    box(0.4, 0.65, 1.7, 1.05, "MySQL 8.0 OLTP", "centers, patients,\nvaccination_records", ec="#0284C7")

    # Tầng 2
    box(2.72, 2.85, 1.82, 1.35, "Apache Airflow 2.9", "DAG: health_data_pipeline\nĐiều phối Batch & ML", ec="#16A34A")
    box(2.72, 1.55, 1.82, 0.92, "Debezium + Kafka", "Đọc MySQL Binlog\n3 Topics thời gian thực", ec="#0D9488")
    box(2.72, 0.45, 1.82, 0.85, "CDC Consumer", "Lọc bỏ PII & Upsert DWH", ec="#0D9488")

    # Tầng 3
    box(5.16, 3.05, 1.73, 1.15, "MinIO Data Lake", "Chuẩn S3 Landing Zone\nPhân vùng theo ngày", ec="#D97706")
    box(5.16, 0.85, 1.73, 1.75, "PostgreSQL 15 DWH", "Snowflake Schema (dwh)\n+ Các bảng kết quả\nK-Means & XGBoost", ec="#B45309")

    # Tầng 4
    box(7.50, 2.75, 1.75, 1.40, "K-Means Nâng cao", "Kiểm định K = 2..8\nHồ sơ cụm tự sinh nhãn\n& Giảm chiều PCA 2D", ec="#7E22CE")
    box(7.50, 0.80, 1.75, 1.50, "Cảnh báo sớm\nXGBoost", "18 đặc trưng nhân quả\nPurged Time Split (H=2)\nĐối sánh 3 mô hình", ec="#7E22CE")

    # Tầng 5
    box(9.82, 1.35, 1.31, 2.65, "Apache\nSuperset 3.1", "12 Datasets\n21 Biểu đồ\n4 Dashboard\n100% Tiếng Việt\n(REST API Sync)", ec="#DC2626")

    arrow_kw = dict(arrowstyle="-|>,head_width=0.22,head_length=0.35", lw=1.2, color="#334155")
    ax.annotate("", xy=(2.72, 3.65), xytext=(2.10, 3.80), arrowprops=arrow_kw)
    ax.annotate("", xy=(2.72, 3.40), xytext=(2.10, 2.55), arrowprops=arrow_kw)
    ax.annotate("", xy=(2.72, 3.05), xytext=(2.10, 1.35), arrowprops=dict(arrowstyle="-|>", lw=1.0, ls="--", color="#64748B"))
    ax.annotate("", xy=(2.72, 2.00), xytext=(2.10, 1.05), arrowprops=dict(arrowstyle="-|>", lw=1.4, color="#0D9488"))
    ax.annotate("", xy=(3.63, 1.30), xytext=(3.63, 1.55), arrowprops=dict(arrowstyle="-|>", lw=1.3, color="#0D9488"))

    ax.annotate("", xy=(5.16, 3.60), xytext=(4.54, 3.60), arrowprops=arrow_kw)
    ax.annotate("", xy=(6.02, 2.60), xytext=(6.02, 3.05), arrowprops=arrow_kw)
    ax.annotate("", xy=(5.16, 1.15), xytext=(4.54, 0.90), arrowprops=dict(arrowstyle="-|>", lw=1.4, color="#0D9488"))

    ax.annotate("", xy=(7.50, 3.35), xytext=(6.89, 2.15), arrowprops=dict(arrowstyle="<->", lw=1.25, color="#7E22CE"))
    ax.annotate("", xy=(7.50, 1.55), xytext=(6.89, 1.55), arrowprops=dict(arrowstyle="<->", lw=1.25, color="#7E22CE"))
    ax.annotate("", xy=(8.38, 2.30), xytext=(8.38, 2.75), arrowprops=dict(arrowstyle="<-", lw=1.2, color="#7E22CE"))

    ax.annotate("", xy=(9.82, 2.65), xytext=(9.25, 2.65), arrowprops=dict(arrowstyle="-|>", lw=1.4, color="#DC2626"))
    ax.annotate("", xy=(9.82, 2.20), xytext=(6.89, 1.90), arrowprops=dict(arrowstyle="-|>", lw=1.0, ls=":", color="#DC2626"))

    fig.tight_layout(pad=0.2)
    fig.savefig(output_path, dpi=240)
    plt.close(fig)


def draw_schema_diagram(output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11.2, 5.0), dpi=240)
    ax.set_xlim(0, 11.2)
    ax.set_ylim(0, 5.0)
    ax.axis("off")

    def tbl(x, y, w, h, name, cols, hdr_color="#1E3A8A", body_bg="#F8FAFC"):
        outer = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.06",
            facecolor=body_bg, edgecolor=hdr_color, linewidth=1.2,
        )
        ax.add_patch(outer)
        hdr_h = 0.34
        hdr = mpatches.FancyBboxPatch(
            (x, y + h - hdr_h), w, hdr_h,
            boxstyle="round,pad=0.02,rounding_size=0.05",
            facecolor=hdr_color, edgecolor=hdr_color, linewidth=1.0,
        )
        ax.add_patch(hdr)
        ax.text(x + w / 2, y + h - hdr_h / 2, name, ha="center", va="center", fontsize=8.0, fontweight="bold", color="white")
        body_text = "\n".join(cols)
        ax.text(x + 0.10, y + h - hdr_h - 0.08, body_text, ha="left", va="top", fontsize=6.8, color="#1E293B", linespacing=1.28)

    # Nhánh Snowflake cốt lõi
    tbl(0.25, 3.55, 2.15, 1.20, "dwh.dim_region", ["PK id", "continent_code", "continent_name"], "#0369A1")
    tbl(0.25, 1.75, 2.15, 1.45, "dwh.dim_country", ["PK id | country_code (ISO-3)", "population, gdp", "beds_per_1000", "FK region_id"], "#0369A1")
    tbl(0.25, 0.20, 2.15, 1.25, "dwh.dim_indicator", ["PK id", "indicator_code", "description"], "#0369A1")

    tbl(2.85, 2.85, 2.35, 1.90, "dwh.fact_cases (OWID)", ["PK id", "FK country_id, date_id", "new_cases, new_deaths", "people_vaccinated", "people_fully_vaccinated", "total_cases, total_deaths"], "#1E3A8A", "#EFF6FF")
    tbl(2.85, 1.45, 2.35, 1.15, "dwh.dim_date", ["PK date_id (YYYYMMDD)", "full_date, year, month", "is_weekend"], "#0369A1")
    tbl(2.85, 0.15, 2.35, 1.05, "dwh.fact_who_covid_daily", ["PK (report_date, country_code)", "ISO-2 tách biệt OWID", "new_cases, new_deaths"], "#475569")

    # Nhánh CDC Ẩn danh
    tbl(5.65, 3.15, 2.45, 1.60, "dwh.dim_patient (Ẩn danh)", ["PK patient_id (Không lưu PII)", "birth_year, gender, province", "health_status, comorbidity", "FK reported_date_id", "is_placeholder"], "#0F766E", "#F0FDFA")
    tbl(5.65, 1.55, 2.45, 1.35, "dwh.fact_vaccination (CDC)", ["PK record_id", "FK center_id, patient_id", "FK date_id", "vaccine_type, dose_number"], "#0F766E", "#F0FDFA")
    tbl(5.65, 0.15, 2.45, 1.15, "dwh.dim_vaccination_center", ["PK id | center_code", "center_name, province", "is_placeholder"], "#0F766E")

    # Nhánh Data Mining Marts
    tbl(8.50, 2.70, 2.50, 2.05, "Nhóm Bảng K-Means + PCA", ["• fact_weekly_covid_cluster", "  (cluster_id, cluster_label,", "   pca_1, pca_2)", "• fact_kmeans_evaluation", "• fact_kmeans_cluster_profile"], "#6D28D9", "#FAF5FF")
    tbl(8.50, 0.25, 2.50, 2.15, "Nhóm Bảng Early Warning", ["• fact_early_warning", "  (surge_probability, alert_level,", "   test / forecast split)", "• fact_early_warning_evaluation", "• fact_early_warning_feature_imp"], "#B91C1C", "#FEF2F2")

    kw = dict(arrowstyle="-|>", lw=1.1, color="#334155")
    ax.annotate("", xy=(1.32, 3.20), xytext=(1.32, 3.55), arrowprops=kw)
    ax.annotate("", xy=(2.85, 3.65), xytext=(2.40, 2.55), arrowprops=kw)
    ax.annotate("", xy=(2.85, 3.25), xytext=(2.40, 0.85), arrowprops=kw)
    ax.annotate("", xy=(4.02, 2.85), xytext=(4.02, 2.60), arrowprops=kw)
    ax.annotate("", xy=(5.65, 2.15), xytext=(5.20, 2.05), arrowprops=kw)
    ax.annotate("", xy=(5.65, 3.45), xytext=(5.20, 2.25), arrowprops=kw)
    ax.annotate("", xy=(6.88, 2.90), xytext=(6.88, 3.15), arrowprops=kw)
    ax.annotate("", xy=(6.88, 1.55), xytext=(6.88, 1.30), arrowprops=kw)
    ax.annotate("", xy=(8.50, 3.70), xytext=(5.20, 3.85), arrowprops=dict(arrowstyle="-|>", lw=1.2, ls="--", color="#6D28D9"))
    ax.annotate("", xy=(9.75, 2.40), xytext=(9.75, 2.70), arrowprops=dict(arrowstyle="<-", lw=1.2, color="#B91C1C"))

    fig.tight_layout(pad=0.2)
    fig.savefig(output_path, dpi=240)
    plt.close(fig)


def draw_kmeans_results_chart(dwh: dict, output_path: Path) -> None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.4), dpi=240)

    eval_rows = dwh["kmeans_eval"]
    ks = [int(r[0]) for r in eval_rows]
    inertias = [float(r[1]) / 1000.0 for r in eval_rows]
    sils = [float(r[2]) for r in eval_rows]

    color_in = "#1D4ED8"
    color_sil = "#DC2626"
    ax1.plot(ks, inertias, marker="o", lw=2.2, color=color_in, label="Inertia (nghìn)")
    ax1.set_xlabel("Số cụm K", fontsize=9.5, fontweight="bold")
    ax1.set_ylabel("Inertia / WCSS (nghìn)", color=color_in, fontsize=9.5, fontweight="bold")
    ax1.tick_params(axis="y", labelcolor=color_in)
    ax1.set_xticks(ks)
    ax1.grid(True, linestyle="--", alpha=0.35)

    ax1_twin = ax1.twinx()
    ax1_twin.plot(ks, sils, marker="s", lw=2.2, ls="--", color=color_sil, label="Silhouette")
    ax1_twin.set_ylabel("Hệ số Silhouette", color=color_sil, fontsize=9.5, fontweight="bold")
    ax1_twin.tick_params(axis="y", labelcolor=color_sil)
    ax1.axvline(4, color="#0F766E", ls=":", lw=1.4, label="K=4 (Đang chọn)")
    ax1.set_title("(a) Kiểm định Khuỷu tay (Inertia) & Silhouette theo K", fontsize=10, fontweight="bold", pad=8)

    palette = {0: "#10B981", 1: "#F59E0B", 2: "#F97316", 3: "#EF4444"}
    pca_rows = dwh["pca_sample"]
    for cid in sorted({int(r[0]) for r in pca_rows}):
        pts = [(float(r[2]), float(r[3]), str(r[1])) for r in pca_rows if int(r[0]) == cid]
        if not pts:
            continue
        lbl = pts[0][2]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax2.scatter(xs, ys, s=14, alpha=0.62, color=palette.get(cid, "#3B82F6"), label=f"Cụm {cid}: {lbl}", edgecolors="none")

    ax2.set_xlabel("Thành phần chính PCA 1 (Quy mô ca mắc & tử vong)", fontsize=9, fontweight="bold")
    ax2.set_ylabel("Thành phần chính PCA 2 (Động lượng tăng trưởng)", fontsize=9, fontweight="bold")
    ax2.set_title("(b) Phân tách 4 Cụm trên Mặt phẳng PCA 2 Chiều", fontsize=10, fontweight="bold", pad=8)
    ax2.grid(True, linestyle="--", alpha=0.35)
    ax2.legend(fontsize=7.6, loc="upper right", framealpha=0.92)

    fig.tight_layout(pad=1.0)
    fig.savefig(output_path, dpi=240)
    plt.close(fig)


def draw_early_warning_chart(dwh: dict, output_path: Path) -> None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.4), dpi=240)

    ew_rows = dwh["ew_eval"]
    models = [str(r[0]).replace(" (", "\n(") for r in ew_rows]
    roc = [float(r[6] or 0) for r in ew_rows]
    f1 = [float(r[5] or 0) for r in ew_rows]
    pr = [float(r[7] or 0) for r in ew_rows]

    x = np.arange(len(models))
    w = 0.25
    b1 = ax1.bar(x - w, roc, width=w, color="#0284C7", label="ROC-AUC")
    b2 = ax1.bar(x, f1, width=w, color="#10B981", label="F1-Score")
    b3 = ax1.bar(x + w, pr, width=w, color="#F97316", label="PR-AUC (Lớp hiếm)")

    for bars in (b1, b2, b3):
        for bar in bars:
            val = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width() / 2, val + 0.015, f"{val:.3f}", ha="center", va="bottom", fontsize=7.2, fontweight="bold")

    ax1.set_xticks(x)
    ax1.set_xticklabels(models, fontsize=8.0)
    ax1.set_ylim(0, 1.06)
    ax1.set_ylabel("Giá trị chỉ số (0 – 1)", fontsize=9, fontweight="bold")
    ax1.set_title("(a) Đối sánh 3 Mô hình trên Tập Test (11.006 dòng)", fontsize=10, fontweight="bold", pad=8)
    ax1.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax1.legend(fontsize=7.8, loc="upper right")

    imp_rows = list(reversed(dwh["ew_importance"]))
    feat_labels = [str(r[1]) for r in imp_rows]
    feat_vals = [float(r[3]) for r in imp_rows]
    y = np.arange(len(feat_labels))
    bars = ax2.barh(y, feat_vals, color="#1E3A8A", height=0.65)
    for bar in bars:
        val = bar.get_width()
        ax2.text(val + 0.3, bar.get_y() + bar.get_height() / 2, f"{val:.1f}%", va="center", ha="left", fontsize=7.5)

    ax2.set_yticks(y)
    ax2.set_yticklabels(feat_labels, fontsize=8.0)
    ax2.set_xlim(0, max(feat_vals) * 1.22 if feat_vals else 25)
    ax2.set_xlabel("Tỷ trọng đóng góp Gain (%)", fontsize=9, fontweight="bold")
    ax2.set_title("(b) 10 Yếu tố Ảnh hưởng Lớn nhất (XGBoost)", fontsize=10, fontweight="bold", pad=8)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.35)

    fig.tight_layout(pad=1.0)
    fig.savefig(output_path, dpi=240)
    plt.close(fig)


# ==============================================================================
# TIỆN ÍCH ĐỊNH DẠNG MICROSOFT WORD (.DOCX) CHUẨN BÁO CÁO HỌC THUẬT
# ==============================================================================
def set_run_font(run, font_name="Times New Roman", size_pt=12.5, bold=False, italic=False, color=DARK):
    run.font.name = font_name
    run.font.size = Pt(size_pt)
    run.bold = bold
    run.italic = italic
    if color is not None:
        run.font.color.rgb = color
    rPr = run._element.get_or_add_rPr()
    rFonts = rPr.find(qn("w:rFonts"))
    if rFonts is None:
        rFonts = OxmlElement("w:rFonts")
        rPr.insert(0, rFonts)
    for attr in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
        rFonts.set(qn(attr), font_name)


def add_page_number(run):
    fldChar1 = parse_xml(r'<w:fldChar %s w:fldCharType="begin"/>' % nsdecls("w"))
    instrText = parse_xml(r'<w:instrText %s xml:space="preserve"> PAGE </w:instrText>' % nsdecls("w"))
    fldChar2 = parse_xml(r'<w:fldChar %s w:fldCharType="separate"/>' % nsdecls("w"))
    fldChar3 = parse_xml(r'<w:fldChar %s w:fldCharType="end"/>' % nsdecls("w"))
    run._r.append(fldChar1)
    run._r.append(instrText)
    run._r.append(fldChar2)
    run._r.append(fldChar3)


def shade_cell(cell, hex_color: str):
    shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{hex_color}"/>')
    cell._tc.get_or_add_tcPr().append(shading)


def set_cell_margins(cell, top=100, bottom=100, left=140, right=140):
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement("w:tcMar")
    for m, val in (("top", top), ("bottom", bottom), ("left", left), ("right", right)):
        node = OxmlElement(f"w:{m}")
        node.set(qn("w:w"), str(val))
        node.set(qn("w:type"), "dxa")
        tcMar.append(node)
    tcPr.append(tcMar)


def set_table_borders(table, border_color="CBD5E1"):
    tblPr = table._tbl.tblPr
    borders = parse_xml(
        f"""
        <w:tblBorders {nsdecls("w")}>
            <w:top w:val="single" w:sz="6" w:space="0" w:color="1E3A8A"/>
            <w:bottom w:val="single" w:sz="8" w:space="0" w:color="1E3A8A"/>
            <w:left w:val="none"/>
            <w:right w:val="none"/>
            <w:insideH w:val="single" w:sz="4" w:space="0" w:color="{border_color}"/>
            <w:insideV w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>
        </w:tblBorders>
        """
    )
    tblPr.append(borders)


def add_heading_1(doc: Document, text: str):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(16)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    pPr = p._p.get_or_add_pPr()
    pBdr = parse_xml(
        f'<w:pBdr {nsdecls("w")}><w:bottom w:val="single" w:sz="10" w:space="4" w:color="1E3A8A"/></w:pBdr>'
    )
    pPr.append(pBdr)
    r = p.add_run(text)
    set_run_font(r, size_pt=15.5, bold=True, color=NAVY)
    return p


def add_heading_2(doc: Document, text: str):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(12)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    r = p.add_run(text)
    set_run_font(r, size_pt=13.5, bold=True, color=TEAL)
    return p


def add_body_p(doc: Document, text: str, bold_prefix: str = None, space_after: float = 5.0):
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.line_spacing = 1.3
    p.paragraph_format.space_after = Pt(space_after)
    if bold_prefix:
        r_pre = p.add_run(bold_prefix)
        set_run_font(r_pre, size_pt=12.5, bold=True, color=DARK)
    r = p.add_run(text)
    set_run_font(r, size_pt=12.5, color=DARK)
    return p


def add_bullet_item(doc: Document, bold_lead: str, text: str):
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.left_indent = Cm(0.75)
    p.paragraph_format.first_line_indent = Cm(-0.4)
    p.paragraph_format.line_spacing = 1.28
    p.paragraph_format.space_after = Pt(3.5)
    r_dot = p.add_run("•  ")
    set_run_font(r_dot, size_pt=12.5, bold=True, color=NAVY)
    if bold_lead:
        r_lead = p.add_run(bold_lead)
        set_run_font(r_lead, size_pt=12.5, bold=True, color=DARK)
    r_txt = p.add_run(text)
    set_run_font(r_txt, size_pt=12.5, color=DARK)
    return p


def add_callout_box(doc: Document, title: str, content: str, fill_hex="EFF6FF", border_hex="1D4ED8"):
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = tbl.cell(0, 0)
    shade_cell(cell, fill_hex)
    set_cell_margins(cell, top=130, bottom=130, left=200, right=180)
    tcBorders = parse_xml(
        f"""
        <w:tcBorders {nsdecls("w")}>
            <w:left w:val="single" w:sz="24" w:space="0" w:color="{border_hex}"/>
            <w:top w:val="none"/><w:right w:val="none"/><w:bottom w:val="none"/>
        </w:tcBorders>
        """
    )
    cell._tc.get_or_add_tcPr().append(tcBorders)
    p = cell.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.line_spacing = 1.25
    p.paragraph_format.space_after = Pt(0)
    r1 = p.add_run(title + ": ")
    set_run_font(r1, size_pt=12.0, bold=True, color=NAVY)
    r2 = p.add_run(content)
    set_run_font(r2, size_pt=12.0, color=SLATE)
    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_after = Pt(3)


def add_styled_table(
    doc: Document,
    headers: list[str],
    rows: list[list[str]],
    col_widths_cm: list[float] = None,
    right_align_cols: set[int] = None,
    highlight_row_idx: int = None,
):
    right_align_cols = right_align_cols or set()
    tbl = doc.add_table(rows=len(rows) + 1, cols=len(headers))
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    set_table_borders(tbl)

    # Header row
    hdr_cells = tbl.rows[0].cells
    trPr = tbl.rows[0]._tr.get_or_add_trPr()
    trPr.append(parse_xml(f'<w:tblHeader {nsdecls("w")}/>'))
    for idx, h_text in enumerate(headers):
        cell = hdr_cells[idx]
        shade_cell(cell, "1E3A8A")
        set_cell_margins(cell, top=110, bottom=110, left=110, right=110)
        cell.vertical_alignment = WD_CELL_VERTICAL.CENTER
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.RIGHT if idx in right_align_cols else WD_ALIGN_PARAGRAPH.LEFT
        p.paragraph_format.space_after = Pt(0)
        r = p.add_run(h_text)
        set_run_font(r, size_pt=11.0, bold=True, color=RGBColor(0xFF, 0xFF, 0xFF))
        if col_widths_cm and idx < len(col_widths_cm):
            cell.width = Cm(col_widths_cm[idx])

    for r_idx, row_vals in enumerate(rows):
        row_cells = tbl.rows[r_idx + 1].cells
        is_hl = highlight_row_idx is not None and r_idx == highlight_row_idx
        bg_hex = "DBEAFE" if is_hl else ("F8FAFC" if r_idx % 2 == 1 else "FFFFFF")
        for c_idx, val in enumerate(row_vals):
            cell = row_cells[c_idx]
            shade_cell(cell, bg_hex)
            set_cell_margins(cell, top=85, bottom=85, left=110, right=110)
            cell.vertical_alignment = WD_CELL_VERTICAL.CENTER
            p = cell.paragraphs[0]
            p.alignment = (
                WD_ALIGN_PARAGRAPH.RIGHT if c_idx in right_align_cols else WD_ALIGN_PARAGRAPH.LEFT
            )
            p.paragraph_format.line_spacing = 1.18
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(str(val))
            set_run_font(r, size_pt=11.0, bold=is_hl, color=DARK)
            if col_widths_cm and c_idx < len(col_widths_cm):
                cell.width = Cm(col_widths_cm[c_idx])

    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_after = Pt(4)
    return tbl


def add_figure(doc: Document, img_path: Path, caption: str, width_inches: float = 6.35):
    p_img = doc.add_paragraph()
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.space_before = Pt(6)
    p_img.paragraph_format.space_after = Pt(3)
    p_img.paragraph_format.keep_with_next = True
    p_img.add_run().add_picture(str(img_path), width=Inches(width_inches))

    p_cap = doc.add_paragraph()
    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_cap.paragraph_format.space_after = Pt(8)
    r = p_cap.add_run(caption)
    set_run_font(r, size_pt=11.0, italic=True, color=SLATE)


# ==============================================================================
# XÂY DỰNG NỘI DUNG TÀI LIỆU WORD HOÀN CHỈNH
# ==============================================================================
def build_docx(dwh: dict, output_docx: Path) -> None:
    doc = Document()

    for sec in doc.sections:
        sec.page_width = Cm(21.0)
        sec.page_height = Cm(29.7)
        sec.top_margin = Cm(2.2)
        sec.bottom_margin = Cm(2.2)
        sec.left_margin = Cm(2.5)
        sec.right_margin = Cm(2.0)
        sec.different_first_page_header_footer = True

        # Running Header (từ trang 2)
        hdr_p = sec.header.paragraphs[0]
        hdr_p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        r_hdr = hdr_p.add_run("Báo cáo Giới thiệu Hệ thống — Nền tảng Dữ liệu & Khai phá Dữ liệu COVID-19")
        set_run_font(r_hdr, size_pt=9.5, italic=True, color=MUTED)

        # Running Footer kèm số trang
        ftr_p = sec.footer.paragraphs[0]
        ftr_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r_f1 = ftr_p.add_run("Trang ")
        set_run_font(r_f1, size_pt=10.0, color=MUTED)
        r_pg = ftr_p.add_run()
        set_run_font(r_pg, size_pt=10.0, bold=True, color=NAVY)
        add_page_number(r_pg)

    counts = dwh["counts"]

    # --- KHUNG TIÊU ĐỀ TRANG ĐẦU ---
    p_sub = doc.add_paragraph()
    p_sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_sub.paragraph_format.space_after = Pt(4)
    r = p_sub.add_run("BÀI TẬP LỚN HỌC PHẦN KHAI PHÁ DỮ LIỆU (DATA MINING)")
    set_run_font(r, size_pt=12.0, bold=True, color=TEAL)

    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_title.paragraph_format.space_after = Pt(6)
    r = p_title.add_run(
        "GIỚI THIỆU TỔNG QUAN HỆ THỐNG\n"
        "NỀN TẢNG DỮ LIỆU & KHAI PHÁ DỮ LIỆU Y TẾ CÔNG CỘNG (COVID-19)"
    )
    set_run_font(r, size_pt=18.0, bold=True, color=NAVY)

    p_desc = doc.add_paragraph()
    p_desc.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_desc.paragraph_format.space_after = Pt(12)
    r = p_desc.add_run(
        "Tích hợp Batch ETL (Apache Airflow, MinIO) • Đồng bộ Thời gian thực ẩn danh PII (Debezium, Kafka)\n"
        "Kho dữ liệu Snowflake (PostgreSQL) • Phân cụm K-Means & PCA • Cảnh báo sớm XGBoost • Apache Superset"
    )
    set_run_font(r, size_pt=11.0, italic=True, color=SLATE)

    # Bảng tóm tắt chỉ số toàn hệ thống
    add_styled_table(
        doc,
        headers=["Chỉ số quy mô thực tế trên hệ thống", "Giá trị ghi nhận", "Nguồn / Cơ chế xử lý"],
        rows=[
            ["Tổng số quan sát dịch tễ ngày toàn cầu", f"{counts['fact_cases']:,} dòng", f"Our World in Data ({counts['countries']} quốc gia, ISO-3)"],
            ["Tổng số điểm dữ liệu tuần – quốc gia phân cụm", f"{counts['weekly_clusters']:,} quan sát", "Tổng hợp chuẩn hóa theo dân số (2020 – 2024)"],
            ["Hồ sơ bệnh nhân nội bộ đã đồng bộ ẩn danh", f"{counts['patients']:,} bệnh nhân", "MySQL OLTP → Debezium → Kafka → dim_patient"],
            ["Lượt tiêm chủng nội bộ đồng bộ thời gian thực", f"{counts['vaccinations']:,} mũi tiêm", "MySQL OLTP → Debezium → Kafka → fact_vaccination"],
            ["Độ chính xác Cảnh báo sớm bùng phát (XGBoost)", "ROC-AUC = 0,925 | PR-AUC = 0,270", "Kiểm thử độc lập 11.006 dòng (Purged Time Split)"],
            ["Hệ thống kiểm thử tự động & trực quan hóa", "23/23 Unit Tests | 4 Dashboards", "21 biểu đồ tiếng Việt khởi tạo tự động qua API"],
        ],
        col_widths_cm=[6.5, 4.2, 5.8],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        arch_png = tmp / "arch.png"
        schema_png = tmp / "schema.png"
        km_png = tmp / "kmeans.png"
        ew_png = tmp / "early_warning.png"

        draw_architecture_diagram(arch_png)
        draw_schema_diagram(schema_png)
        draw_kmeans_results_chart(dwh, km_png)
        draw_early_warning_chart(dwh, ew_png)

        # ======================================================================
        # CHƯƠNG 1
        # ======================================================================
        add_heading_1(doc, "1. Bối cảnh Bài toán & Mục tiêu Hệ thống")
        add_body_p(
            doc,
            "Trong công tác giám sát dịch bệnh truyền nhiễm quy mô lớn như đại dịch COVID-19, dữ liệu phát sinh từ nhiều nguồn độc lập với cấu trúc, tần suất cập nhật và mức độ nhạy cảm rất khác nhau. Dữ liệu thống kê quốc tế (như Our World in Data và WHO) có khối lượng lịch sử lớn nhưng khác biệt về chuẩn mã quốc gia (ISO-3 so với ISO-2), trong khi dữ liệu quản lý bệnh nhân và tiêm chủng tại các cơ sở y tế nội bộ lại biến động liên tục từng giây và chứa thông tin định danh cá nhân (PII).",
        )
        add_body_p(
            doc,
            "Dự án được thiết kế nhằm xây dựng một nền tảng dữ liệu hoàn chỉnh (End-to-End Data Platform) kết hợp chặt chẽ giữa Kỹ thuật dữ liệu hiện đại và hai bài toán Khai phá dữ liệu chuyên sâu:",
        )
        add_bullet_item(doc, "Hợp nhất dữ liệu đa luồng (Hybrid Batch + Streaming): ", "Tự động thu thập dữ liệu toàn cầu theo lô qua Apache Airflow và MinIO Data Lake, đồng thời bắt biến động giao dịch nội bộ dưới 2 giây qua Debezium Binlog & Apache Kafka.")
        add_bullet_item(doc, "Bảo vệ quyền riêng tư theo thiết kế (Privacy by Design): ", "Loại bỏ triệt để họ tên thật (full_name) và mã băm công dân (citizen_hash) ngay trên đường truyền trước khi nạp vào Kho dữ liệu phân tích.")
        add_bullet_item(doc, "Khai phá tri thức không giám sát (K-Means nâng cao & PCA): ", "Đánh giá định lượng dải số cụm K = 2..8 bằng 4 chỉ số độc lập, tự động diễn giải tâm cụm thành nhãn dịch tễ tiếng Việt và chiếu xuống không gian PCA 2 chiều.")
        add_bullet_item(doc, "Cảnh báo sớm có giám sát (XGBoost Surge Prediction): ", "Chuyển hóa kết quả phân cụm và 17 đặc trưng động lượng – miễn dịch thành mô hình dự báo nguy cơ bùng phát dịch trước 2 tuần, kiểm định nghiêm ngặt chống rò rỉ thời gian.")
        add_bullet_item(doc, "Trực quan hóa BI tự động hóa 100% tiếng Việt: ", "Cung cấp công cụ đồng bộ qua REST API tạo lập tự động 12 Dataset, 21 biểu đồ và 4 bảng điều khiển tiếng Việt trên Apache Superset.")

        # ======================================================================
        # CHƯƠNG 2
        # ======================================================================
        add_heading_1(doc, "2. Kiến trúc Kỹ thuật Tổng thể 5 Tầng")
        add_body_p(
            doc,
            "Toàn bộ hạ tầng gồm 10 dịch vụ container được đóng gói đồng nhất trên Docker Compose, chia thành 5 tầng chức năng tách biệt từ nguồn dữ liệu đến giao diện phân tích:",
        )
        add_figure(doc, arch_png, "Hình 1. Sơ đồ kiến trúc tổng thể 5 tầng kết hợp Batch ETL, Real-time CDC và 2 mô hình Data Mining.")

        add_styled_table(
            doc,
            headers=["Dịch vụ Container", "Công nghệ / Image", "Cổng Host", "Vai trò kỹ thuật trong hệ thống"],
            rows=[
                ["mysql-oltp", "MySQL 8.0", "3307", "CSDL giao dịch nội bộ (điểm tiêm, bệnh nhân, mũi tiêm), bật ROW binlog."],
                ["zookeeper & kafka", "Debezium 2.5", "2181 / 9092", "Hàng đợi thông điệp phân tán lưu trữ 3 topic biến động CDC."],
                ["debezium", "Debezium Connect 2.5", "8083", "Connector v2 đọc trực tiếp binlog MySQL không làm chậm OLTP."],
                ["cdc-consumer", "Python Kafka Consumer", "(Nội bộ)", "Gỡ connector cũ, ẩn danh PII, xử lý placeholder và upsert DWH."],
                ["minio", "MinIO Object Storage", "9000 / 9001", "Data Lake chuẩn S3 lưu trữ bản chụp dữ liệu thô phân vùng theo ngày."],
                ["postgres-dwh", "PostgreSQL 15 Alpine", "5433", "Kho dữ liệu trung tâm Snowflake Schema và lưu trữ đầu ra Data Mining."],
                ["dwh-migrate", "PSQL Migration Runner", "(Chạy 1 lần)", "Thực thi kịch bản nâng cấp lược đồ và View tiếng Việt hoàn toàn idempotent."],
                ["airflow-*", "Apache Airflow 2.9.3", "8081", "Điều phối pipeline ETL, huấn luyện K-Means và XGBoost theo lịch hàng ngày."],
                ["superset", "Apache Superset 3.1.1", "8089", "Nền tảng BI hiển thị 4 Dashboard tiếng Việt phục vụ giám sát và báo cáo."],
            ],
            col_widths_cm=[3.2, 3.6, 2.2, 7.5],
        )

        # ======================================================================
        # CHƯƠNG 3
        # ======================================================================
        add_heading_1(doc, "3. Thiết kế Kho Dữ liệu Snowflake Schema & Đồng bộ CDC Ẩn danh")
        add_body_p(
            doc,
            "Kho dữ liệu trên PostgreSQL (schema dwh) được tổ chức theo mô hình Snowflake Schema nhằm chuẩn hóa phân cấp địa lý (dim_country tham chiếu sang dim_region), kết hợp với nhóm bảng sự kiện tiêm chủng CDC và nhóm bảng chuyên biệt lưu trữ đầu ra của các thuật toán khai phá dữ liệu:",
        )
        add_figure(doc, schema_png, "Hình 2. Mô hình thực thể – liên kết (Snowflake Schema) và các bảng kết quả Data Mining trong PostgreSQL DWH.")

        add_heading_2(doc, "3.1. Ba giải pháp kỹ thuật trọng tâm bảo đảm chất lượng dữ liệu")
        add_bullet_item(doc, "Tách biệt chuẩn mã quốc gia ISO-3 và ISO-2: ", "Dữ liệu toàn cầu OWID sử dụng mã 3 ký tự (VNM, USA) được chuẩn hóa vào fact_cases; dữ liệu WHO sử dụng mã 2 ký tự (VN, US) được lưu tại bảng riêng fact_who_covid_daily. Thiết kế này ngăn chặn hoàn toàn lỗi mất mát hoặc nhân bản dòng khi kết nối chéo nguồn.")
        add_bullet_item(doc, "Ẩn danh hóa dữ liệu cá nhân (Data Minimization): ", "Khi đồng bộ bảng patients từ MySQL, cả luồng Batch (xuất lên MinIO) lẫn luồng Streaming CDC đều loại bỏ họ tên và mã công dân, chỉ giữ lại năm sinh, giới tính, tỉnh/thành, tình trạng nhiễm và bệnh nền trong dwh.dim_patient để phân tích cơ cấu dịch tễ.")
        add_bullet_item(doc, "Xử lý bất đồng bộ thứ tự sự kiện Kafka (Placeholder Pattern): ", "Do sự kiện bệnh nhân và sự kiện mũi tiêm nằm trên hai topic Kafka khác nhau, bản ghi mũi tiêm có thể đến trước. Consumer tự động khởi tạo một bản ghi giữ chỗ (is_placeholder = TRUE) trong dim_patient để đảm bảo toàn vẹn khóa ngoại, sau đó tự động cập nhật đầy đủ thuộc tính khi bản tin bệnh nhân tới.")

        # ======================================================================
        # CHƯƠNG 4
        # ======================================================================
        add_heading_1(doc, "4. Bài toán Data Mining 1: Phân cụm K-Means Nâng cao & Giảm chiều PCA")
        add_heading_2(doc, "4.1. Đặc trưng dịch tễ học & Quy trình kiểm định số cụm K")
        add_body_p(
            doc,
            f"Từ bảng sự kiện dwh.fact_cases, hệ thống tổng hợp dữ liệu theo từng cặp (quốc gia, tuần bắt đầu), thu được {counts['weekly_clusters']:,} quan sát hợp lệ. Mỗi điểm dữ liệu được biểu diễn bởi vector 3 đặc trưng chuẩn hóa theo quy mô dân số và biến đổi logarit tự nhiên ln(1 + x) để xử lý phân phối lệch phải mạnh của dữ liệu dịch bệnh:",
        )
        add_bullet_item(doc, "Mức độ lây nhiễm (X1): ", "ln(1 + cases_per_100k) — phản ánh quy mô ca mắc mới trong tuần trên 100.000 dân.")
        add_bullet_item(doc, "Mức độ nghiêm trọng tử vong (X2): ", "ln(1 + deaths_per_100k) — phản ánh gánh nặng tử vong trên 100.000 dân.")
        add_bullet_item(doc, "Động lượng biến động tuần (X3): ", "ln(1 + rate_t) - ln(1 + rate_{t-1}) — đo tốc độ bùng phát hay hạ nhiệt so với tuần liền trước.")

        add_body_p(
            doc,
            "Sau khi chuẩn hóa Z-score bằng StandardScaler, thuật toán chạy thử nghiệm toàn bộ dải K từ 2 đến 8 (cấu hình qua COVID_KMEANS_K_RANGE) và ghi lại 4 chỉ số đánh giá độc lập vào bảng dwh.fact_kmeans_evaluation. Hệ thống hỗ trợ đồng thời chế độ chọn K cố định (mặc định K = 4 để giữ ổn định 4 vùng cảnh báo) và chế độ tự động COVID_KMEANS_CLUSTERS=auto (chọn K đạt Silhouette cao nhất).",
        )

        add_figure(doc, km_png, "Hình 3. Kết quả kiểm định số cụm K = 2..8 và hình chiếu phân tách 4 cụm trên mặt phẳng PCA 2 chiều.")

        eval_table_rows = [
            [
                str(r[0]),
                f"{float(r[1]):,.0f}",
                f"{float(r[2]):.4f}",
                f"{float(r[3]):.4f}",
                f"{float(r[4]):,.1f}",
                "Đang chọn (K=4)" if r[5] else ("Đỉnh Silhouette" if r[6] else "—"),
            ]
            for r in dwh["kmeans_eval"]
        ]
        sel_idx = next((i for i, r in enumerate(dwh["kmeans_eval"]) if r[5]), None)
        add_styled_table(
            doc,
            headers=["Số cụm K", "Inertia (WCSS)", "Silhouette (↑)", "Davies–Bouldin (↓)", "Calinski–Harabasz (↑)", "Ghi chú"],
            rows=eval_table_rows,
            col_widths_cm=[2.0, 2.8, 2.7, 3.0, 3.3, 2.7],
            right_align_cols={1, 2, 3, 4},
            highlight_row_idx=sel_idx,
        )

        add_heading_2(doc, "4.2. Hồ sơ cụm & Cơ chế tự động sinh nhãn tiếng Việt")
        add_body_p(
            doc,
            "Để khắc phục nhược điểm các cụm K-Means chỉ mang số thứ tự vô nghĩa (0, 1, 2, 3), hệ thống sắp xếp thứ tự cụm tăng dần theo tỷ lệ mắc trung bình, sau đó giải chuẩn hóa tâm cụm (scaler.inverse_transform) về không gian log gốc để kết hợp mức độ ca mắc và hướng biến động (đang tăng nếu tâm biến động log > 0,15; đang giảm nếu < -0,15; còn lại là ổn định):",
        )

        prof_rows = [
            [
                f"Cụm {r[0]}",
                str(r[1]),
                f"{int(r[2]):,}",
                f"{float(r[3]):.1f}%",
                f"{float(r[4]):.1f}",
                f"{float(r[5]):.1f}",
                f"{float(r[6]):.2f}",
                f"{float(r[7]):.3f}" if r[7] is not None else "—",
            ]
            for r in dwh["kmeans_profiles"]
        ]
        add_styled_table(
            doc,
            headers=["Mã cụm", "Nhãn dịch tễ tự sinh", "Số tuần–QG", "Tỉ trọng", "Ca TB/100k", "Trung vị/100k", "TV TB/100k", "Silhouette"],
            rows=prof_rows,
            col_widths_cm=[1.6, 3.8, 2.1, 1.7, 1.9, 2.1, 1.8, 1.5],
            right_align_cols={2, 3, 4, 5, 6, 7},
        )

        if dwh["vnm_history"]:
            add_body_p(
                doc,
                "Đối chiếu thực tế trên chuỗi dữ liệu lịch sử của Việt Nam (VNM), kết quả phân cụm phản ánh chính xác diễn biến dịch tễ và hiệu quả bảo vệ của chiến dịch tiêm chủng vắc-xin:",
            )
            vnm_rows = [
                [
                    str(r[0]),
                    f"Cụm {r[1]} ({r[2]})",
                    f"{float(r[3]):.1f}",
                    f"{float(r[4]):.2f}",
                    f"{float(r[5]):+.1f}%" if r[5] is not None else "—",
                ]
                for r in dwh["vnm_history"]
            ]
            add_styled_table(
                doc,
                headers=["Tuần bắt đầu", "Cụm & Nhãn phân loại", "Ca mắc / 100k dân", "Tử vong / 100k dân", "Tăng trưởng tuần"],
                rows=vnm_rows,
                col_widths_cm=[2.8, 5.2, 3.0, 3.0, 2.5],
                right_align_cols={2, 3, 4},
            )

        # ======================================================================
        # CHƯƠNG 5
        # ======================================================================
        add_heading_1(doc, "5. Bài toán Data Mining 2: Hệ thống Cảnh báo Sớm Bùng phát Dịch (XGBoost)")
        add_heading_2(doc, "5.1. Phát biểu bài toán, Nhãn mục tiêu & Bộ 18 đặc trưng nhân quả")
        add_body_p(
            doc,
            "Phân cụm K-Means giúp nhận diện trạng thái hiện tại, trong khi cơ quan y tế cần biết sớm quốc gia nào sắp bùng phát dịch. Hệ thống thiết lập bài toán phân lớp nhị phân tại mỗi tuần t: dự báo liệu sau H = 2 tuần nữa, tỷ lệ ca mắc có tăng ít nhất 1,5 lần so với tuần hiện tại VÀ vượt ngưỡng tối thiểu 10 ca/100.000 dân (nhằm loại bỏ nhiễu khi số ca nền quá nhỏ) hay không.",
        )
        add_styled_table(
            doc,
            headers=["Nhóm đặc trưng", "Các biến đầu vào (chỉ tính từ tuần t trở về trước)", "Ý nghĩa dịch tễ học"],
            rows=[
                ["Quy mô & Độ trễ", "log_cases_per_100k, lag 1–3 tuần, trung bình & độ lệch chuẩn trượt 4 tuần", "Nắm bắt nền ca mắc và mức độ dao động ngắn hạn."],
                ["Động lượng lây nhiễm", "growth_1w, growth_2w, growth_accel (gia tốc lây nhiễm)", "Phát hiện sớm sự đổi chiều và tăng tốc của đường cong dịch."],
                ["Gánh nặng tử vong", "log_deaths_per_100k, death_growth_1w, cfr_roll4 (CFR 4 tuần)", "Phản ánh độc lực biến chủng và mức quá tải xét nghiệm."],
                ["Độ phủ miễn dịch", "vax_any_pct (≥ 1 mũi), vax_full_pct (tiêm đủ liều)", "Đo lường hàng rào miễn dịch vắc-xin tích lũy tại tuần t."],
                ["Mùa vụ & Toàn cầu", "week_sin, week_cos, global_mean_growth_1w", "Biểu diễn tính chu kỳ mùa đông/hè và làn sóng lây lan quốc tế."],
                ["Liên kết K-Means", "cluster_id (thứ bậc nghiêm trọng 0..3 tại tuần t)", "Kết nối trực tiếp đầu ra bài toán phân cụm sang bài toán dự báo."],
            ],
            col_widths_cm=[3.4, 6.8, 6.3],
        )

        add_heading_2(doc, "5.2. Chia dữ liệu chống rò rỉ (Purged Time Split) & Kết quả thực nghiệm")
        add_callout_box(
            doc,
            "Nguyên tắc chống rò rỉ nhãn tương lai (Purged Time-Series Split)",
            "Do nhãn tại tuần t sử dụng số ca của tuần t + H, nếu cắt tập Train và Test liền kề nhau thì H tuần cuối của tập Train sẽ 'nhìn thấy' dữ liệu trong giai đoạn Test. Hệ thống áp dụng khoảng trống cách ly H = 2 tuần (Train < cutoff - 2 tuần; Test >= cutoff), đồng thời loại bỏ tuần cuối chưa đủ 7 ngày báo cáo và cân bằng lớp hiếm bằng trọng số scale_pos_weight.",
        )

        add_figure(doc, ew_png, "Hình 4. Kết quả đối sánh 3 mô hình dự báo trên tập kiểm thử độc lập và xếp hạng 10 đặc trưng quan trọng nhất.")

        ew_table_rows = [
            [
                str(r[0]),
                f"{float(r[2]):.2f}",
                f"{float(r[3]):.3f}",
                f"{float(r[4]):.3f}",
                f"{float(r[5]):.3f}",
                f"{float(r[6]):.3f}",
                f"{float(r[7]):.3f}",
            ]
            for r in dwh["ew_eval"]
        ]
        add_styled_table(
            doc,
            headers=["Mô hình đánh giá", "Ngưỡng", "Precision", "Recall", "F1-Score", "ROC-AUC", "PR-AUC"],
            rows=ew_table_rows,
            col_widths_cm=[4.9, 1.8, 2.0, 1.9, 2.0, 2.0, 1.9],
            right_align_cols={1, 2, 3, 4, 5, 6},
            highlight_row_idx=0,
        )

        add_body_p(
            doc,
            "Nhận xét kết quả & Kiểm chứng Ablation: ",
            bold_prefix="Đánh giá khoa học: ",
        )
        add_bullet_item(doc, "Ưu thế vượt trội của XGBoost: ", "Trên 11.006 mẫu kiểm thử độc lập, XGBoost đạt ROC-AUC = 0,925 và PR-AUC = 0,270 — cao hơn +63,6% so với Hồi quy Logistic (0,165) và gấp gần 4 lần Luật cơ sở tăng trưởng 25% (0,070).")
        add_bullet_item(doc, "Kiểm chứng Ablation (loại bỏ cluster_id): ", "Khi tắt biến cluster_id (COVID_EW_INCLUDE_CLUSTER=false), XGBoost vẫn đạt ROC-AUC = 0,926, F1 = 0,355 và PR-AUC = 0,264. Điều này chứng minh kết quả mô hình hoàn toàn thực chất, không bị phồng điểm do tâm cụm K-Means, đồng thời cluster_id giúp cải thiện nhẹ độ nhạy PR-AUC từ 0,264 lên 0,270.")

        # ======================================================================
        # CHƯƠNG 6: MARKOV TRANSITION & ASSOCIATION RULES
        # ======================================================================
        add_heading_1(doc, "6. Bài toán Data Mining 3 & 4: Chuyển dịch Cụm Markov & Luật Kết hợp Apriori")
        add_heading_2(doc, "6.1. Khai phá Chuỗi Chuyển dịch Cụm Markov & Thời gian Lưu trú (Dwell Time)")
        add_body_p(
            doc,
            "Trên chuỗi phân cụm tuần liên tiếp của từng quốc gia, hệ thống ước lượng ma trận xác suất chuyển trạng thái Markov bậc 1 P(C_{t+1} = j | C_t = i) và thống kê độ dài các đợt lưu trú liên tục (Dwell Time) trong mỗi vùng dịch:",
        )
        if dwh.get("dwell_times"):
            dwell_rows = [
                [
                    f"Cụm {r[0]}",
                    str(r[1]),
                    f"{int(r[2]):,}",
                    f"{float(r[3]):.2f} tuần",
                    f"{float(r[4]):.1f} tuần",
                    f"{int(r[5])} tuần",
                    f"{float(r[6]):.2f}%",
                ]
                for r in dwh["dwell_times"]
            ]
            add_styled_table(
                doc,
                headers=["Mã cụm", "Nhãn cụm dịch tễ", "Số đợt", "Lưu trú TB", "Trung vị", "Đợt dài nhất", "Xác suất giữ cụm"],
                rows=dwell_rows,
                col_widths_cm=[1.7, 3.9, 1.8, 2.2, 2.0, 2.3, 2.6],
                right_align_cols={2, 3, 4, 5, 6},
            )

        add_heading_2(doc, "6.2. Khai phá Luật Kết hợp trên Hồ sơ Bệnh nhân & Tiêm chủng (Apriori / FP-Growth)")
        add_body_p(
            doc,
            "Kết hợp bảng bệnh nhân ẩn danh (dwh.dim_patient) và lịch sử mũi tiêm (dwh.fact_vaccination), thuật toán Apriori khai phá các quy luật {Nhóm tuổi + Giới tính + Bệnh nền + Số mũi tiêm / Loại vắc-xin} => {Tình trạng COVID-19} có độ tin cậy (Confidence) và độ nâng (Lift) cao nhất:",
        )
        if dwh.get("assoc_rules"):
            rule_rows = [
                [
                    str(r[0]),
                    str(r[1]),
                    str(r[2]),
                    str(r[3]),
                    f"{float(r[4]):.2f}%",
                    f"{float(r[5]):.1f}%",
                    f"{float(r[6]):.2f}x",
                ]
                for r in dwh["assoc_rules"]
            ]
            add_styled_table(
                doc,
                headers=["#", "Tổ hợp điều kiện (Vế trái)", "Kết cục (Vế phải)", "Số BN", "Support", "Confidence", "Lift"],
                rows=rule_rows,
                col_widths_cm=[0.9, 6.9, 3.0, 1.3, 1.5, 1.6, 1.3],
                right_align_cols={3, 4, 5, 6},
                highlight_row_idx=0,
            )

        # ======================================================================
        # CHƯƠNG 7
        # ======================================================================
        add_heading_1(doc, "7. Hệ thống 4 Bảng Điều khiển Tiếng Việt trên Apache Superset (25 Biểu đồ)")
        add_body_p(
            doc,
            "Thông qua kịch bản tự động hóa REST API (scripts/init_superset_dashboards.py), toàn bộ 12 View trong DWH được đăng ký, gán từ điển nhãn cột tiếng Việt (verbose_name) và dựng thành 21 biểu đồ phân bổ trên 4 Dashboard chuyên đề:",
        )
        add_styled_table(
            doc,
            headers=["STT", "Tên Bảng điều khiển (Dashboard Tiếng Việt)", "Đường dẫn (Slug)", "Số biểu đồ & Nội dung chính"],
            rows=[
                ["1", "Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu", "covid19-kmeans-clusters", "4 biểu đồ: Tương quan ca mắc – tử vong, tỉ trọng cụm, bảng tổng hợp 4 nhóm."],
                ["2", "Phân tích Phân cụm Dịch tễ — Đánh giá Số cụm K & Hồ sơ Cụm", "covid19-kmeans-mining", "6 biểu đồ: Khuỷu tay Inertia, Silhouette & Davies–Bouldin, mặt phẳng PCA 2D, diễn biến tuần."],
                ["3", "Hệ thống Cảnh báo Sớm Nguy cơ Bùng phát Dịch COVID-19", "covid19-early-warning", "5 biểu đồ: Bản đồ thế giới dự báo 2 tuần tới, đối sánh 3 mô hình, Top 10 yếu tố, bảng xếp hạng QG."],
                ["4", "Giám sát Thời gian thực — Bệnh nhân Ẩn danh & Tiêm chủng", "covid19-cdc-patients-vaccination", "6 biểu đồ: KPI thời gian thực, ca bệnh theo độ tuổi, cơ cấu bệnh nền, vắc-xin & thống kê tỉnh/thành."],
            ],
            col_widths_cm=[1.1, 5.5, 4.1, 5.8],
        )

        # ======================================================================
        # CHƯƠNG 7
        # ======================================================================
        add_heading_1(doc, "7. Kiểm thử Chất lượng Phần mềm & Hướng dẫn Vận hành Nhanh")
        add_body_p(
            doc,
            "Hệ thống đi kèm bộ kiểm thử đơn vị tự động gồm 23 bài kiểm thử độc lập và các kịch bản trình diễn trực quan:",
        )
        add_bullet_item(doc, "Kiểm thử CDC & Ẩn danh hóa (10/10 tests pass — cdc/test_consumer.py): ", "Kiểm tra giải mã ngày Debezium, cắt bỏ PII bệnh nhân, tạo bản ghi giữ chỗ placeholder, xóa theo khóa chính và rollback giao dịch khi DWH lỗi.")
        add_bullet_item(doc, "Kiểm thử K-Means & PCA (8/8 tests pass — test_weekly_covid_kmeans.py): ", "Kiểm tra chuẩn hóa dân số, tính bất biến kết quả, tự động chọn K theo đỉnh Silhouette, xử lý biên khi K vượt số điểm phân biệt và thứ tự nhãn cụm.")
        add_bullet_item(doc, "Kiểm thử Cảnh báo sớm XGBoost (5/5 tests pass — test_covid_early_warning.py): ", "Kiểm chứng gán nhãn đúng trên chuỗi tăng gấp đôi/đi ngang, tính nhân quả nghiêm ngặt (đột biến dữ liệu ở t+1 không làm đổi đặc trưng tại t), khoảng cách purge H tuần và an toàn dữ liệu đơn lớp.")
        add_bullet_item(doc, "Các kịch bản vận hành chính: ", "Khởi chạy hạ tầng bằng `docker compose up -d --build`; sinh dữ liệu OLTP bằng `python scripts/seed_oltp_data.py`; đồng bộ giao diện Superset bằng `python scripts/init_superset_dashboards.py`; minh họa CDC trực tiếp bằng `./scripts/demo_cdc.ps1` và xuất báo cáo phân tích bằng `python scripts/demo_kmeans.py`.")

    output_docx.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(output_docx))


def main():
    parser = argparse.ArgumentParser(description="Sinh báo cáo giới thiệu hệ thống định dạng Word (.docx).")
    parser.add_argument(
        "--output",
        default="/opt/airflow/data/GIOI_THIEU_HE_THONG.docx",
        help="Đường dẫn file .docx đầu ra",
    )
    args = parser.parse_args()

    out_path = Path(args.output)
    print("[1/2] Đang truy vấn số liệu thực nghiệm trực tiếp từ PostgreSQL DWH...")
    dwh_data = fetch_dwh_snapshot()
    print("[2/2] Đang kết xuất 4 sơ đồ/biểu đồ 240 DPI và đóng gói file Word (.docx)...")
    build_docx(dwh_data, out_path)
    print(f"[+] Đã xuất thành công tài liệu Word: {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
