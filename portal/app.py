# -*- coding: utf-8 -*-
"""
Cổng Web Tương tác AI & Giám sát Dịch tễ COVID-19 (Streamlit Portal)
Kết nối trực tiếp PostgreSQL DWH + Mô hình XGBoost thật + Kafka Realtime Anomaly Alerts.
"""

import math
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
import xgboost as xgb

st.set_page_config(
    page_title="Cổng AI Phân tích & Cảnh báo Dịch tễ COVID-19",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Tinh chỉnh khoảng cách (spacing), thẻ KPI, thanh Tab và độ thoáng bảng biểu
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.6rem;
            padding-bottom: 2.5rem;
            max-width: 1440px;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 10px;
            margin-bottom: 0.85rem;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 10px 18px;
            border-radius: 8px 8px 0 0;
            font-weight: 600;
        }
        [data-testid="stMetric"] {
            background: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 14px 18px;
        }
        h3 {
            margin-top: 0.4rem !important;
            margin-bottom: 0.6rem !important;
        }
        h4 {
            margin-top: 0.5rem !important;
            margin-bottom: 0.5rem !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

DWH_CONFIG = {
    "host": os.getenv("DWH_HOST", "postgres-dwh"),
    "port": int(os.getenv("DWH_PORT", "5432")),
    "dbname": os.getenv("DWH_DB", "health_dwh"),
    "user": os.getenv("DWH_USER", "dwh_admin"),
    "password": os.getenv("DWH_PASSWORD", "dwh_password"),
}
MODEL_PATH = Path(os.getenv("EARLY_WARNING_MODEL_PATH", "/app/data/models/early_warning_model.pkl"))

FEATURE_LABELS_VI = {
    "log_cases_per_100k": "Mức ca mắc hiện tại",
    "log_cases_lag_1": "Ca mắc trễ 1 tuần",
    "log_cases_lag_2": "Ca mắc trễ 2 tuần",
    "log_cases_lag_3": "Ca mắc trễ 3 tuần",
    "cases_roll4_mean": "TB ca mắc 4 tuần",
    "cases_roll4_std": "Độ biến động 4 tuần",
    "growth_1w": "Tốc độ tăng 1 tuần",
    "growth_2w": "Tốc độ tăng 2 tuần",
    "growth_accel": "Gia tốc lây nhiễm",
    "log_deaths_per_100k": "Mức tử vong hiện tại",
    "death_growth_1w": "Tăng trưởng tử vong",
    "cfr_roll4": "Tỷ lệ tử vong / ca mắc (CFR)",
    "vax_any_pct": "Độ phủ tiêm ≥ 1 mũi",
    "vax_full_pct": "Độ phủ tiêm đủ liều",
    "week_sin": "Mùa vụ (Sin tuần)",
    "week_cos": "Mùa vụ (Cos tuần)",
    "global_mean_growth_1w": "Xu hướng tăng toàn cầu",
    "cluster_id": "Nhóm cụm K-Means",
}


@st.cache_data(ttl=20)
def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(**DWH_CONFIG)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()


@st.cache_resource(ttl=60)
def load_ew_artifact():
    if not MODEL_PATH.exists():
        return None
    with MODEL_PATH.open("rb") as f:
        return pickle.load(f)


def _strip_item_prefix(label: str) -> str:
    return label.split(": ", 1)[-1] if ": " in label else label


# ==============================================================================
# HEADER & BANNER CẢNH BÁO Ổ DỊCH THỜI GIAN THỰC (KAFKA CDC STREAMING)
# ==============================================================================
st.markdown(
    """
    <div style="background: linear-gradient(90deg, #1E3A8A 0%, #0F766E 100%);
                padding: 20px 26px; border-radius: 12px; color: white; margin-bottom: 18px;">
        <h2 style="margin: 0; color: white; font-size: 24px; line-height: 1.35;">
            🧬 Cổng Tương tác AI & Giám sát Dịch tễ COVID-19
        </h2>
        <p style="margin: 6px 0 0 0; color: #E2E8F0; font-size: 14px; line-height: 1.5;">
            Phân cụm K-Means & PCA • Cảnh báo sớm XGBoost + TreeSHAP • Chuỗi Markov • Luật Kết hợp Apriori • Isolation Forest & Cảnh báo Ổ dịch Kafka
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

try:
    rt_alerts = query_df(
        """
        SELECT alert_id,
               TO_CHAR(detected_at AT TIME ZONE 'Asia/Ho_Chi_Minh', 'YYYY-MM-DD HH24:MI:SS') AS detected_time_vn,
               province,
               confirmed_count,
               ROUND(baseline_avg::numeric, 2) AS baseline_avg,
               ROUND(z_score::numeric, 2) AS z_score,
               severity_vi,
               alert_message_vi
        FROM dwh.vw_realtime_anomalies
        ORDER BY detected_at DESC
        LIMIT 10
        """
    )
    if not rt_alerts.empty:
        latest = rt_alerts.iloc[0]
        st.error(
            f"🚨 **CẢNH BÁO Ổ DỊCH THỜI GIAN THỰC (KAFKA CDC) — [{latest['severity_vi']}]**: "
            f"{latest['alert_message_vi']} *(Ghi nhận lúc {latest['detected_time_vn']} giờ VN)*"
        )
except Exception:
    rt_alerts = pd.DataFrame()

with st.sidebar:
    st.header("⚙️ Điều khiển nhanh")
    if st.button("🔄 Làm mới dữ liệu DWH", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.divider()
    st.markdown("**Liên kết nhanh hệ thống:**")
    st.markdown("- [Apache Superset (4 Dashboard)](http://localhost:8089)")
    st.markdown("- [Apache Airflow (Điều phối DAG)](http://localhost:8081)")
    st.markdown("- [MinIO Data Lake (Landing Zone)](http://localhost:9001)")
    st.divider()
    st.caption("Đồ án Kho dữ liệu & Khai phá dữ liệu — Nhóm KNH (PTIT)")

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "🎯 Mô phỏng What-If (XGBoost & TreeSHAP)",
        "🩺 Tra cứu Nguy cơ Bệnh nhân (Apriori)",
        "📈 Quỹ đạo Quốc gia & Dự báo Markov",
        "🚨 Phát hiện Bất thường (Isolation Forest & Kafka)",
    ]
)

# ==============================================================================
# TAB 1: MÔ PHỎNG KỊCH BẢN WHAT-IF TRỰC TIẾP VỚI MÔ HÌNH XGBOOST + TREESHAP
# ==============================================================================
with tab1:
    st.subheader("Mô phỏng Kịch bản “What-If” trên Mô hình XGBoost & Giải thích Cục bộ TreeSHAP")
    st.caption(
        "Thay đổi giả định dịch tễ ở cột trái để chạy suy luận trực tiếp trên mô hình cảnh báo sớm "
        "và quan sát mức đóng góp tăng / giảm rủi ro của từng đặc trưng."
    )
    artifact = load_ew_artifact()

    col_left, col_right = st.columns([1.05, 1.25], gap="large")
    with col_left:
        st.markdown("#### 1. Thiết lập thông số đầu vào tại tuần hiện tại")
        cases_100k = st.slider("Số ca mắc tuần này (trên 100.000 dân)", 0.0, 1500.0, 45.0, step=5.0)
        growth_1w_pct = st.slider("Tốc độ tăng trưởng ca so với tuần trước (%)", -80.0, 300.0, 45.0, step=5.0)
        growth_2w_pct = st.slider("Tốc độ tăng trưởng ca so với 2 tuần trước (%)", -80.0, 400.0, 70.0, step=5.0)
        deaths_100k = st.slider("Số ca tử vong tuần này (trên 100.000 dân)", 0.0, 20.0, 0.5, step=0.1)
        vax_full_pct = st.slider("Độ phủ tiêm chủng đầy đủ (% dân số)", 0, 100, 35, step=5)
        global_trend = st.slider("Xu hướng tăng trung bình toàn cầu cùng tuần (%)", -40.0, 100.0, 15.0, step=5.0)
        cluster_id = st.selectbox(
            "Nhóm mức dịch K-Means hiện tại",
            options=[0, 1, 2, 3],
            index=1,
            format_func=lambda x: {
                0: "Cụm 0: Thấp – ổn định",
                1: "Cụm 1: Trung bình – ổn định",
                2: "Cụm 2: Cao – đang tăng",
                3: "Cụm 3: Rất cao – ổn định",
            }[x],
        )

    with col_right:
        st.markdown("#### 2. Dự báo nguy cơ sau 2 tuần & Phân rã TreeSHAP")
        if artifact is None:
            st.warning(
                "Chưa tìm thấy tệp mô hình `/app/data/models/early_warning_model.pkl`. "
                "Vui lòng chạy DAG `covid_early_warning` trên Airflow để xuất mô hình."
            )
        else:
            estimator = artifact["estimator"]
            threshold = float(artifact["decision_threshold"])
            feature_cols = list(artifact["feature_cols"])

            log_c = math.log1p(cases_100k)
            prev1 = cases_100k / max(0.05, 1.0 + growth_1w_pct / 100.0)
            prev2 = cases_100k / max(0.05, 1.0 + growth_2w_pct / 100.0)
            prev3 = prev2
            log_lag1, log_lag2, log_lag3 = math.log1p(prev1), math.log1p(prev2), math.log1p(prev3)
            roll4_vals = np.array([log_c, log_lag1, log_lag2, log_lag3])

            g1 = log_c - log_lag1
            g2 = log_c - log_lag2
            g_prev = log_lag1 - log_lag2
            accel = g1 - g_prev
            log_d = math.log1p(deaths_100k)
            cfr = min(1.0, deaths_100k / max(cases_100k, 1.0)) if cases_100k > 0 else 0.0
            vax_full = vax_full_pct / 100.0
            vax_any = min(1.0, vax_full + 0.08)

            feat_map = {
                "log_cases_per_100k": log_c,
                "log_cases_lag_1": log_lag1,
                "log_cases_lag_2": log_lag2,
                "log_cases_lag_3": log_lag3,
                "cases_roll4_mean": float(np.mean(roll4_vals)),
                "cases_roll4_std": float(np.std(roll4_vals)),
                "growth_1w": g1,
                "growth_2w": g2,
                "growth_accel": accel,
                "log_deaths_per_100k": log_d,
                "death_growth_1w": 0.0,
                "cfr_roll4": cfr,
                "vax_any_pct": vax_any,
                "vax_full_pct": vax_full,
                "week_sin": 0.0,
                "week_cos": 1.0,
                "global_mean_growth_1w": math.log1p(max(-0.9, global_trend / 100.0)),
                "cluster_id": float(cluster_id),
            }
            X_input = np.array([[feat_map[c] for c in feature_cols]], dtype=float)
            prob = float(estimator.predict_proba(X_input)[0, 1])

            if prob >= threshold:
                level, badge_color = "NGUY CƠ CAO", "#EF4444"
            elif prob >= threshold / 2.0:
                level, badge_color = "CẦN THEO DÕI", "#F59E0B"
            else:
                level, badge_color = "NGUY CƠ THẤP", "#10B981"

            st.markdown(
                f"""
                <div style="background:#F8FAFC; border:2px solid {badge_color}; border-radius:10px; padding:16px 20px; margin-bottom:14px;">
                    <div style="font-size:13px; color:#475569; font-weight:600; letter-spacing:0.3px;">
                        XÁC SUẤT BÙNG PHÁT DỊCH SAU 2 TUẦN (MÔ HÌNH XGBOOST)
                    </div>
                    <div style="display:flex; align-items:baseline; gap:14px; flex-wrap:wrap; margin-top:6px;">
                        <span style="font-size:38px; font-weight:800; color:{badge_color};">{prob * 100:.1f}%</span>
                        <span style="font-size:16px; font-weight:700; color:{badge_color};">{level}</span>
                        <span style="font-size:13px; color:#64748B;">(Ngưỡng báo động: {threshold * 100:.0f}%)</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Biểu đồ đóng góp cục bộ TreeSHAP
            if hasattr(estimator, "get_booster"):
                dmat = xgb.DMatrix(X_input, feature_names=feature_cols)
                contribs = estimator.get_booster().predict(dmat, pred_contribs=True)[0, : len(feature_cols)]
                shap_df = pd.DataFrame(
                    {
                        "Đặc trưng": [FEATURE_LABELS_VI.get(c, c) for c in feature_cols],
                        "Đóng góp SHAP": contribs,
                    }
                )
                shap_df["abs"] = shap_df["Đóng góp SHAP"].abs()
                shap_top = shap_df.sort_values("abs", ascending=False).head(8).sort_values("Đóng góp SHAP")
                shap_top["Hướng tác động"] = np.where(
                    shap_top["Đóng góp SHAP"] >= 0, "Đẩy nguy cơ tăng", "Kéo nguy cơ giảm"
                )
                fig_shap = px.bar(
                    shap_top,
                    x="Đóng góp SHAP",
                    y="Đặc trưng",
                    color="Hướng tác động",
                    orientation="h",
                    color_discrete_map={
                        "Đẩy nguy cơ tăng": "#EF4444",
                        "Kéo nguy cơ giảm": "#10B981",
                    },
                    title="Top 8 Đặc trưng Quyết định Xác suất Dự báo (Giá trị TreeSHAP log-odds)",
                )
                fig_shap.update_layout(
                    height=370,
                    margin=dict(l=12, r=16, t=46, b=16),
                    legend_title_text="Chiều tác động",
                    legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
                )
                st.plotly_chart(fig_shap, use_container_width=True)

    st.divider()
    st.markdown("#### 3. Bảng Xếp hạng Cảnh báo Sớm Thực tế theo Quốc gia kèm Giải thích TreeSHAP")
    st.caption("Nguồn dữ liệu: `dwh.vw_early_warning_latest` (Kỳ dự báo mới nhất cho từng quốc gia có ca mắc hoạt động).")
    try:
        ew_latest = query_df(
            """
            SELECT country_code AS "Mã QG",
                   country_name AS "Quốc gia",
                   week_start AS "Tuần dự báo",
                   ROUND(cases_per_100k::numeric, 1) AS "Ca mắc / 100k dân",
                   ROUND((surge_probability * 100)::numeric, 1) AS "Xác suất bùng phát (%)",
                   alert_level_vi AS "Mức cảnh báo",
                   top_reasons_vi AS "Giải thích TreeSHAP (3 yếu tố chính)"
            FROM dwh.vw_early_warning_latest
            WHERE model_version = 'early-warning-v1' AND cases_per_100k > 0
            ORDER BY surge_probability DESC
            LIMIT 15
            """
        )
        st.dataframe(ew_latest, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.info(f"Chưa tải được bảng cảnh báo: {exc}")

# ==============================================================================
# TAB 2: TRA CỨU NGUY CƠ BỆNH NHÂN THEO LUẬT KẾT HỢP (APRIORI)
# ==============================================================================
with tab2:
    st.subheader("Tra cứu Nguy cơ Cá nhân theo Luật Kết hợp Dịch tễ (Apriori / FP-Growth)")
    st.caption(
        "Chọn thông tin nhân khẩu học, bệnh nền và lịch sử tiêm chủng để đối chiếu trực tiếp với tập luật kết hợp "
        "khai phá từ hồ sơ bệnh nhân ẩn danh."
    )

    row1_c1, row1_c2, row1_c3 = st.columns(3, gap="medium")
    age_sel = row1_c1.selectbox(
        "Nhóm tuổi",
        ["Tuổi: Dưới 18", "Tuổi: 18 – 39", "Tuổi: 40 – 59", "Tuổi: Từ 60 trở lên"],
        index=3,
        format_func=_strip_item_prefix,
    )
    gender_sel = row1_c2.selectbox(
        "Giới tính",
        ["Giới tính: Nam", "Giới tính: Nữ"],
        index=0,
        format_func=_strip_item_prefix,
    )
    comorb_sel = row1_c3.selectbox(
        "Bệnh nền",
        [
            "Bệnh nền: Không có",
            "Bệnh nền: Tăng huyết áp",
            "Bệnh nền: Tiểu đường",
            "Bệnh nền: Tim mạch",
            "Bệnh nền: Hô hấp mạn tính",
            "Bệnh nền: Béo phì",
        ],
        index=1,
        format_func=_strip_item_prefix,
    )

    row2_c1, row2_c2 = st.columns(2, gap="medium")
    dose_sel = row2_c1.selectbox(
        "Trạng thái tiêm chủng",
        [
            "Tiêm chủng: Chưa tiêm (0 mũi)",
            "Tiêm chủng: Mới tiêm 1 mũi",
            "Tiêm chủng: Đã tiêm đủ 2 mũi",
            "Tiêm chủng: Đã tiêm nhắc lại (≥ 3 mũi)",
        ],
        index=1,
        format_func=_strip_item_prefix,
    )
    vax_sel = row2_c2.selectbox(
        "Loại vắc-xin chính (nếu đã tiêm)",
        ["Không có", "Vắc-xin: Pfizer-BioNTech", "Vắc-xin: AstraZeneca", "Vắc-xin: Moderna", "Vắc-xin: VeroCell"],
        index=2,
        format_func=_strip_item_prefix,
    )

    user_items = {age_sel, gender_sel, comorb_sel, dose_sel}
    if vax_sel != "Không có" and "0 mũi" not in dose_sel:
        user_items.add(vax_sel)

    rules_df = query_df(
        """
        SELECT rule_id, antecedents, target_category_vi, support_count,
               ROUND(support_pct::numeric, 2) AS support_pct,
               ROUND(confidence_pct::numeric, 1) AS confidence_pct,
               ROUND(lift::numeric, 2) AS lift,
               ROUND(conviction::numeric, 2) AS conviction,
               rule_text_vi
        FROM dwh.vw_patient_association_rules
        WHERE model_version = 'apriori-v1'
        ORDER BY lift DESC, confidence_pct DESC
        """
    )

    matched = []
    for row in rules_df.itertuples(index=False):
        ant_set = {part.strip() for part in str(row.antecedents).split("+")}
        if ant_set.issubset(user_items):
            matched.append(row)

    st.write("")
    if matched:
        matched_df = pd.DataFrame(matched)
        top_r = matched_df.iloc[0]
        if top_r.target_category_vi == "Dương tính COVID-19":
            st.error(
                f"⚠️ **CẢNH BÁO NGUY CƠ CAO**: Hồ sơ khớp **{len(matched_df)} luật kết hợp**! "
                f"Luật mạnh nhất: **{top_r.rule_text_vi}** — Độ tin cậy **{top_r.confidence_pct:.1f}%**, "
                f"Độ nâng **Lift = {top_r.lift:.2f}x**. Khuyến nghị ưu tiên tiêm đủ liều / nhắc lại và tầm soát sớm."
            )
        else:
            st.success(
                f"✅ Hồ sơ khớp **{len(matched_df)} luật kết hợp** (nhóm nguy cơ thấp / ổn định): "
                f"**{top_r.rule_text_vi}** — Độ tin cậy **{top_r.confidence_pct:.1f}%**, Độ nâng **Lift = {top_r.lift:.2f}x**."
            )
        st.dataframe(
            matched_df[
                ["rule_id", "antecedents", "target_category_vi", "support_count", "support_pct", "confidence_pct", "lift"]
            ].rename(
                columns={
                    "rule_id": "Thứ hạng",
                    "antecedents": "Tổ hợp điều kiện khớp",
                    "target_category_vi": "Kết cục dự báo",
                    "support_count": "Số bệnh nhân",
                    "support_pct": "Độ phổ biến (%)",
                    "confidence_pct": "Độ tin cậy (%)",
                    "lift": "Độ nâng (Lift)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Hồ sơ vừa chọn không nằm trong tập luật nguy cơ đột biến (mức rủi ro tương đương nền chung).")

# ==============================================================================
# TAB 3: QUỸ ĐẠO QUỐC GIA & MÔ PHỎNG MARKOV k TUẦN (P^k)
# ==============================================================================
with tab3:
    st.subheader("Quỹ đạo Phân cụm Lịch sử & Mô phỏng Chuyển dịch Chuỗi Markov")
    st.caption("Theo dõi lịch sử chuyển dịch giữa 4 vùng dịch K-Means, điểm bất thường Isolation Forest và phân phối xác suất sau k tuần.")
    countries_df = query_df(
        "SELECT DISTINCT country_code, country_name FROM dwh.vw_weekly_covid_clusters ORDER BY country_name"
    )
    default_idx = (
        int(countries_df.index[countries_df["country_code"] == "VNM"][0])
        if "VNM" in set(countries_df["country_code"])
        else 0
    )
    sel_country = st.selectbox(
        "Chọn quốc gia cần khảo sát quỹ đạo dịch tễ",
        options=countries_df["country_code"].tolist(),
        index=default_idx,
        format_func=lambda code: f"{countries_df.loc[countries_df['country_code'] == code, 'country_name'].values[0]} ({code})",
    )

    c_hist = query_df(
        """
        SELECT week_start, cases_per_100k, deaths_per_100k, cluster_id, cluster_label
        FROM dwh.vw_weekly_covid_clusters
        WHERE country_code = %s AND model_version = 'weekly-kmeans-v1'
        ORDER BY week_start
        """,
        params=(sel_country,),
    )
    c_anom = query_df(
        """
        SELECT week_start, cases_per_100k, anomaly_type_vi, explanation_vi
        FROM dwh.vw_covid_anomalies
        WHERE country_code = %s AND model_version = 'iforest-v1'
        ORDER BY week_start
        """,
        params=(sel_country,),
    )

    country_label = countries_df.loc[countries_df["country_code"] == sel_country, "country_name"].values[0]
    fig_traj = px.scatter(
        c_hist,
        x="week_start",
        y="cases_per_100k",
        color="cluster_label",
        color_discrete_map={
            "Thấp – ổn định": "#10B981",
            "Trung bình – ổn định": "#F59E0B",
            "Cao – đang tăng": "#F97316",
            "Rất cao – ổn định": "#EF4444",
        },
        title=f"Diễn biến Ca mắc trên 100.000 dân theo Cụm K-Means & Điểm Bất thường — {country_label} ({sel_country})",
        labels={"week_start": "Tuần bắt đầu", "cases_per_100k": "Ca mắc / 100.000 dân", "cluster_label": "Nhãn cụm dịch tễ"},
    )
    if not c_anom.empty:
        fig_traj.add_trace(
            go.Scatter(
                x=c_anom["week_start"],
                y=c_anom["cases_per_100k"],
                mode="markers",
                name="Bất thường (Isolation Forest)",
                marker=dict(symbol="x", size=11, color="#0F172A", line=dict(width=1.5)),
                text=c_anom["anomaly_type_vi"],
                hovertemplate="<b>Bất thường:</b> %{text}<br>Tuần: %{x}<br>Ca mắc / 100k: %{y:.1f}<extra></extra>",
            )
        )
    fig_traj.update_layout(height=400, margin=dict(l=16, r=16, t=48, b=16))
    st.plotly_chart(fig_traj, use_container_width=True)

    st.divider()
    st.markdown("#### Dự phóng Xác suất Chuyển trạng thái Markov sau k tuần")
    mk_col1, mk_col2 = st.columns([1, 2], gap="large")
    with mk_col1:
        start_c = st.selectbox(
            "Trạng thái cụm xuất phát ở tuần hiện tại",
            options=[0, 1, 2, 3],
            index=3,
            format_func=lambda x: {
                0: "Cụm 0: Thấp – ổn định",
                1: "Cụm 1: Trung bình – ổn định",
                2: "Cụm 2: Cao – đang tăng",
                3: "Cụm 3: Rất cao – ổn định",
            }[x],
        )
        k_steps = st.slider("Số tuần dự phóng tiếp theo (k tuần)", 1, 16, 4)

    with mk_col2:
        trans_df = query_df(
            """
            SELECT from_cluster_id, to_cluster_id, to_cluster_label, transition_probability
            FROM dwh.vw_cluster_transition
            WHERE model_version = 'weekly-kmeans-v1'
            ORDER BY from_cluster_id, to_cluster_id
            """
        )
        if not trans_df.empty:
            n_c = int(trans_df["from_cluster_id"].max()) + 1
            P = np.zeros((n_c, n_c), dtype=float)
            lbl_map = {}
            for row in trans_df.itertuples(index=False):
                P[int(row.from_cluster_id), int(row.to_cluster_id)] = float(row.transition_probability)
                lbl_map[int(row.to_cluster_id)] = str(row.to_cluster_label)
            Pk = np.linalg.matrix_power(P, k_steps)
            probs = Pk[start_c] * 100.0
            proj_df = pd.DataFrame(
                {
                    "Cụm đích": [lbl_map.get(i, f"Cụm {i}") for i in range(n_c)],
                    "Xác suất (%)": probs,
                }
            )
            fig_mk = px.bar(
                proj_df,
                x="Cụm đích",
                y="Xác suất (%)",
                text=proj_df["Xác suất (%)"].map(lambda v: f"{v:.1f}%"),
                color="Cụm đích",
                color_discrete_map={
                    "Thấp – ổn định": "#10B981",
                    "Trung bình – ổn định": "#F59E0B",
                    "Cao – đang tăng": "#F97316",
                    "Rất cao – ổn định": "#EF4444",
                },
                title=f"Phân bố xác suất trạng thái sau {k_steps} tuần xuất phát từ Cụm {start_c}",
            )
            fig_mk.update_traces(textposition="outside")
            fig_mk.update_layout(
                height=320,
                showlegend=False,
                yaxis_range=[0, min(112, max(probs) * 1.18 + 8)],
                margin=dict(l=16, r=16, t=46, b=16),
            )
            st.plotly_chart(fig_mk, use_container_width=True)

# ==============================================================================
# TAB 4: PHÁT HIỆN BẤT THƯỜNG (ISOLATION FOREST + KAFKA REALTIME STREAMING)
# ==============================================================================
with tab4:
    st.subheader("1. Cảnh báo Ổ dịch Bất thường Thời gian thực từ Luồng Kafka CDC")
    st.caption("Nguồn dữ liệu: `dwh.vw_realtime_anomalies` — Giám sát cửa sổ trượt 300 giây các ca dương tính theo Tỉnh / Thành phố.")
    if rt_alerts.empty:
        st.info(
            r"Chưa có ổ dịch bất thường thời gian thực nào. Chạy `.\scripts\demo_realtime_outbreak.ps1` "
            "trên PowerShell rồi nhấn **Làm mới dữ liệu DWH** ở thanh bên trái để xem cảnh báo cập nhật trực tiếp!"
        )
    else:
        st.dataframe(
            rt_alerts.rename(
                columns={
                    "alert_id": "Mã cảnh báo",
                    "detected_time_vn": "Thời điểm phát hiện (Giờ VN)",
                    "province": "Tỉnh / Thành phố",
                    "confirmed_count": "Ca dương tính (trong cửa sổ)",
                    "baseline_avg": "Nền TB các tỉnh khác",
                    "z_score": "Chỉ số Z-Score",
                    "severity_vi": "Mức độ khẩn cấp",
                    "alert_message_vi": "Nội dung cảnh báo ổ dịch",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.subheader("2. Các Tuần Bất thường Dịch tễ Toàn cầu Phát hiện bởi Isolation Forest")
    st.caption("Nguồn dữ liệu: `dwh.vw_covid_anomalies` — Quét 56.477 quan sát tuần – quốc gia để tách biệt đột biến dịch thật và nhiễu dồn báo cáo.")
    anom_all = query_df(
        """
        SELECT week_start,
               country_code,
               country_name,
               ROUND(cases_per_100k::numeric, 1) AS cases_per_100k,
               ROUND(deaths_per_100k::numeric, 2) AS deaths_per_100k,
               ROUND(anomaly_score::numeric, 4) AS anomaly_score,
               anomaly_type_vi,
               explanation_vi
        FROM dwh.vw_covid_anomalies
        WHERE model_version = 'iforest-v1'
        ORDER BY anomaly_score DESC
        LIMIT 200
        """
    )
    if not anom_all.empty:
        c_m1, c_m2, c_m3 = st.columns(3, gap="medium")
        c_m1.metric("Số tuần bất thường (Top 200)", f"{len(anom_all):,}")
        c_m2.metric("Đột biến ca mắc cục bộ", f"{int((anom_all['anomaly_type_vi'] == 'Đột biến ca mắc cục bộ').sum()):,}")
        c_m3.metric("Tử vong tăng dị thường", f"{int((anom_all['anomaly_type_vi'] == 'Tử vong tăng dị thường').sum()):,}")

        st.write("")
        sel_types = st.multiselect(
            "Lọc theo nhóm phân loại bất thường",
            options=sorted(anom_all["anomaly_type_vi"].unique().tolist()),
            default=sorted(anom_all["anomaly_type_vi"].unique().tolist()),
        )
        filtered_anom = anom_all[anom_all["anomaly_type_vi"].isin(sel_types)]
        st.dataframe(
            filtered_anom.rename(
                columns={
                    "week_start": "Tuần bắt đầu",
                    "country_code": "Mã QG",
                    "country_name": "Quốc gia",
                    "cases_per_100k": "Ca mắc / 100k dân",
                    "deaths_per_100k": "Tử vong / 100k dân",
                    "anomaly_score": "Điểm bất thường",
                    "anomaly_type_vi": "Phân loại bất thường",
                    "explanation_vi": "Diễn giải chi tiết",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
