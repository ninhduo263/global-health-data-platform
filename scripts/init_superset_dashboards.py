# -*- coding: utf-8 -*-
"""
Tự động khởi tạo, đồng bộ và Việt hóa 100% toàn bộ Datasets (12 views DWH),
Charts (21 biểu đồ) và 4 Dashboards trên Apache Superset thông qua REST API (/api/v1/...).

Đặc điểm thiết kế:
- Chỉ dùng thư viện chuẩn Python (urllib, http.cookiejar, json, uuid, argparse) ->
  chạy trực tiếp trên Windows Host không cần cài thêm package, không bị chặn bởi App Control.
- Việt hóa toàn diện mọi thành phần trong Dashboard:
  1. Tên Dashboard & Tiêu đề phân khu thuyết minh (Markdown).
  2. Tên từng Biểu đồ, tên Trục X/Y, tên Chỉ số đo lường (Metric).
  3. Nhãn hiển thị cột (verbose_name) của cả 12 Dataset (tiêu đề cột bảng, bộ lọc, chú giải).
  4. Giá trị dữ liệu phân loại bên trong biểu đồ thông qua các cột *_vi của DWH View.
- Hoàn toàn Idempotent: tự động cập nhật tại chỗ (in-place) cả tên biểu đồ cũ sang tên tiếng Việt
  mà không tạo bản ghi trùng lặp.
"""

import argparse
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

IN_DOCKER = os.path.exists("/.dockerenv") or os.environ.get("DWH_HOST") == "postgres-dwh"
DEFAULT_SUPERSET_URL = os.getenv(
    "SUPERSET_URL",
    "http://superset:8088" if IN_DOCKER else "http://localhost:8089",
)
DEFAULT_USERNAME = os.getenv("SUPERSET_ADMIN_USER", "admin")
DEFAULT_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DEFAULT_DB_NAME = os.getenv("SUPERSET_DATABASE_NAME", "Health DWH")
DEFAULT_DWH_URI = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'dwh_admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'dwh_password')}@postgres-dwh:5432/"
    f"{os.getenv('POSTGRES_DB', 'health_dwh')}"
)

ALL_DWH_VIEWS = [
    ("vw_dashboard_cases", "report_date"),
    ("vw_who_covid_daily", "report_date"),
    ("vw_dashboard_vaccinations", "vaccination_date"),
    ("vw_dashboard_patients", "reported_date"),
    ("vw_vaccination_by_patient", "vaccination_date"),
    ("vw_weekly_covid_clusters", "week_start"),
    ("vw_kmeans_evaluation", None),
    ("vw_kmeans_cluster_profile", None),
    ("vw_cluster_transition", None),
    ("vw_cluster_dwell_time", None),
    ("vw_early_warning", "week_start"),
    ("vw_early_warning_latest", "week_start"),
    ("vw_early_warning_evaluation", None),
    ("vw_early_warning_feature_importance", None),
    ("vw_patient_association_rules", None),
]

DATASET_COLUMN_PUT_KEYS = {
    "advanced_data_type",
    "column_name",
    "description",
    "expression",
    "extra",
    "filterable",
    "groupby",
    "id",
    "is_active",
    "is_dttm",
    "python_date_format",
    "type",
    "uuid",
    "verbose_name",
}

# Từ điển ánh xạ tên cột kỹ thuật sang nhãn tiếng Việt dễ đọc trên bảng, trục và chú giải
COLUMN_VERBOSE_NAMES = {
    # Thời gian & Địa lý
    "report_date": "Ngày báo cáo",
    "reported_date": "Ngày khai báo",
    "vaccination_date": "Ngày tiêm chủng",
    "week_start": "Tuần bắt đầu",
    "year": "Năm",
    "month": "Tháng",
    "country_code": "Mã quốc gia",
    "country_name": "Quốc gia",
    "continent_name": "Châu lục",
    "who_region": "Khu vực WHO",
    "province": "Tỉnh / Thành phố",
    "center_province": "Tỉnh / Thành điểm tiêm",
    "center_code": "Mã điểm tiêm",
    "center_name": "Tên cơ sở tiêm chủng",
    # Số liệu dịch tễ & Tiêm chủng
    "population": "Dân số",
    "new_cases": "Ca mắc mới",
    "new_deaths": "Ca tử vong mới",
    "total_cases": "Tổng ca mắc lũy kế",
    "total_deaths": "Tổng tử vong lũy kế",
    "cumulative_cases": "Tổng ca mắc lũy kế",
    "cumulative_deaths": "Tổng tử vong lũy kế",
    "new_vaccinations": "Số mũi tiêm mới",
    "total_vaccinations": "Tổng mũi tiêm lũy kế",
    "people_vaccinated": "Số người đã tiêm ≥ 1 mũi",
    "people_fully_vaccinated": "Số người đã tiêm đủ liều",
    "weekly_cases": "Số ca mắc trong tuần",
    "weekly_deaths": "Số ca tử vong trong tuần",
    "cases_per_100k": "Ca mắc / 100.000 dân",
    "deaths_per_100k": "Tử vong / 100.000 dân",
    "case_growth_pct": "Tốc độ tăng ca tuần (%)",
    # CDC Bệnh nhân & Tiêm chủng nội bộ
    "vaccine_type": "Loại vắc-xin",
    "dose_number": "Mũi tiêm thứ",
    "dose_label_vi": "Liều tiêm",
    "vaccination_count": "Số lượt tiêm",
    "patient_count": "Số bệnh nhân",
    "gender": "Mã giới tính",
    "gender_vi": "Giới tính",
    "age_group": "Mã nhóm tuổi",
    "age_group_vi": "Nhóm tuổi",
    "health_status": "Mã tình trạng",
    "health_status_vi": "Tình trạng sức khỏe",
    "comorbidity": "Mã bệnh nền",
    "comorbidity_vi": "Bệnh nền",
    # K-Means Clustering
    "k": "Số cụm K",
    "cluster_id": "Mã cụm",
    "cluster_count": "Tổng số cụm",
    "cluster_label": "Nhãn cụm dịch tễ",
    "pca_1": "Trục phân cụm 1 (PCA 1)",
    "pca_2": "Trục phân cụm 2 (PCA 2)",
    "inertia": "Độ phân tán nội cụm (Inertia)",
    "silhouette": "Hệ số tách cụm (Silhouette)",
    "davies_bouldin": "Chỉ số Davies–Bouldin",
    "calinski_harabasz": "Chỉ số Calinski–Harabasz",
    "total_rows": "Tổng số dòng dữ liệu",
    "silhouette_sample_rows": "Cỡ mẫu tính Silhouette",
    "is_selected": "Đang được chọn",
    "is_best_silhouette": "Silhouette cao nhất",
    "member_count": "Số tuần – quốc gia",
    "member_share": "Tỉ trọng cụm",
    "avg_cases_per_100k": "Ca mắc TB / 100.000 dân",
    "median_cases_per_100k": "Ca mắc trung vị / 100.000 dân",
    "avg_deaths_per_100k": "Tử vong TB / 100.000 dân",
    "median_case_growth_pct": "Tăng trưởng trung vị (%)",
    "centroid_log_cases": "Tâm cụm log(Ca mắc)",
    "centroid_log_deaths": "Tâm cụm log(Tử vong)",
    "centroid_log_change": "Tâm cụm biến động log",
    "avg_silhouette": "Độ gắn kết cụm (Silhouette TB)",
    # Cảnh báo sớm (Early Warning)
    "model_version": "Phiên bản mô hình",
    "model_name": "Mã mô hình",
    "model_name_vi": "Mô hình dự báo",
    "is_primary": "Mô hình chính",
    "horizon_weeks": "Số tuần dự báo trước",
    "prediction_split": "Mã phân tập",
    "prediction_split_vi": "Phân tập dữ liệu",
    "surge_probability": "Xác suất bùng phát",
    "decision_threshold": "Ngưỡng cảnh báo",
    "predicted_surge": "Dự báo bùng phát",
    "alert_level": "Mã cảnh báo",
    "alert_level_vi": "Mức cảnh báo",
    "actual_surge": "Thực tế bùng phát",
    "future_cases_per_100k": "Ca mắc thực tế sau 2 tuần / 100k",
    "accuracy": "Độ chính xác (Accuracy)",
    "precision_score": "Độ chuẩn xác (Precision)",
    "recall_score": "Độ phủ phát hiện (Recall)",
    "f1_score": "Điểm cân bằng F1",
    "roc_auc": "Chỉ số ROC-AUC",
    "pr_auc": "Chỉ số PR-AUC",
    "train_rows": "Số dòng huấn luyện",
    "test_rows": "Số dòng kiểm thử",
    "feature_name": "Mã đặc trưng",
    "feature_name_vi": "Đặc trưng đầu vào",
    "importance": "Mức độ đóng góp",
    "importance_rank": "Thứ hạng quan trọng",
    "computed_at": "Thời điểm tính toán",
    # Khai phá Chuyển dịch Cụm Markov & Dwell Time
    "from_cluster_id": "Mã cụm nguồn",
    "from_cluster_label": "Cụm tuần hiện tại (t)",
    "to_cluster_id": "Mã cụm đích",
    "to_cluster_label": "Cụm tuần kế tiếp (t+1)",
    "transition_pair_vi": "Chiều chuyển dịch cụm",
    "transition_count": "Số lần chuyển cụm",
    "from_total_count": "Tổng số tuần ở cụm nguồn",
    "transition_probability": "Xác suất chuyển cụm",
    "transition_prob_pct": "Xác suất chuyển cụm (%)",
    "is_self_transition": "Giữ nguyên cụm",
    "transition_type_vi": "Xu hướng chuyển dịch",
    "episode_count": "Số đợt lưu trú",
    "total_weeks": "Tổng số tuần",
    "avg_dwell_weeks": "Số tuần lưu trú TB / đợt",
    "median_dwell_weeks": "Số tuần lưu trú trung vị",
    "max_dwell_weeks": "Đợt dài nhất (tuần)",
    "retention_probability": "Xác suất duy trì cụm",
    "retention_prob_pct": "Xác suất duy trì cụm (%)",
    # Khai phá Luật kết hợp (Apriori / FP-Growth)
    "rule_id": "Thứ hạng luật",
    "antecedents": "Tổ hợp điều kiện (Vế trái)",
    "consequent": "Kết cục sức khỏe (Vế phải)",
    "rule_text_vi": "Phát biểu Luật kết hợp",
    "antecedent_count": "Số điều kiện kết hợp",
    "support_count": "Số bệnh nhân khớp luật",
    "total_transactions": "Tổng số bệnh nhân",
    "support": "Độ phổ biến (Support)",
    "support_pct": "Độ phổ biến Support (%)",
    "confidence": "Độ tin cậy (Confidence)",
    "confidence_pct": "Độ tin cậy Confidence (%)",
    "lift": "Độ nâng (Lift)",
    "leverage": "Độ đòn bẩy (Leverage)",
    "conviction": "Độ thuyết phục (Conviction)",
    "target_category_vi": "Nhóm kết cục sức khỏe",
}


class SupersetApiClient:
    """Lightweight Superset REST API v1 client using Python standard library."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )
        self.access_token = None
        self.csrf_token = None

    def authenticate(self) -> None:
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True,
        }
        resp = self._raw_request("POST", "/api/v1/security/login", payload, auth=False)
        self.access_token = resp["access_token"]

        try:
            csrf_resp = self._raw_request("GET", "/api/v1/security/csrf_token/")
            self.csrf_token = csrf_resp.get("result")
        except Exception:
            self.csrf_token = None

    def _raw_request(
        self,
        method: str,
        path: str,
        body: dict = None,
        query: dict = None,
        auth: bool = True,
    ) -> dict:
        url = f"{self.base_url}{path}"
        if query:
            encoded_q = urllib.parse.urlencode(
                {
                    k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
                    for k, v in query.items()
                }
            )
            url = f"{url}?{encoded_q}"

        data_bytes = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        if auth and self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if auth and self.csrf_token and method in ("POST", "PUT", "DELETE"):
            headers["X-CSRFToken"] = self.csrf_token
            headers["Referer"] = self.base_url

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=30) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Superset API {method} {path} failed ({exc.code}): {err_body}"
            ) from exc

    def list_all(self, endpoint: str, page_size: int = 500) -> list[dict]:
        res = self._raw_request(
            "GET",
            endpoint,
            query={"q": {"page": 0, "page_size": page_size}},
        )
        return res.get("result") or []

    def ensure_database(self, database_name: str, sqlalchemy_uri: str) -> int:
        items = [
            item
            for item in self.list_all("/api/v1/database/")
            if item.get("database_name") == database_name
        ]
        if items:
            return int(items[0]["id"])

        created = self._raw_request(
            "POST",
            "/api/v1/database/",
            {
                "database_name": database_name,
                "sqlalchemy_uri": sqlalchemy_uri,
                "expose_in_sqllab": True,
                "allow_ctas": False,
                "allow_cvas": False,
                "allow_dml": False,
            },
        )
        return int(created["id"])

    def ensure_dataset(
        self,
        database_id: int,
        schema: str,
        table_name: str,
        main_dttm_col: str = None,
        existing_datasets: dict = None,
    ) -> dict:
        key = (schema, table_name)
        if existing_datasets is not None and key in existing_datasets:
            dataset_id = int(existing_datasets[key]["id"])
        else:
            matches = [
                item
                for item in self.list_all("/api/v1/dataset/")
                if item.get("table_name") == table_name
                and item.get("schema") == schema
                and int((item.get("database") or {}).get("id", database_id)) == database_id
            ]
            if matches:
                dataset_id = int(matches[0]["id"])
            else:
                created = self._raw_request(
                    "POST",
                    "/api/v1/dataset/",
                    {
                        "database": database_id,
                        "schema": schema,
                        "table_name": table_name,
                    },
                )
                dataset_id = int(created["id"])

        # Đồng bộ cột mới từ PostgreSQL DWH
        self._raw_request("PUT", f"/api/v1/dataset/{dataset_id}/refresh")

        detail = self._raw_request("GET", f"/api/v1/dataset/{dataset_id}").get("result") or {}
        raw_columns = detail.get("columns") or []
        col_names = {c.get("column_name") for c in raw_columns}

        updated_columns = []
        needs_col_update = False
        for col in raw_columns:
            clean_col = {k: col[k] for k in DATASET_COLUMN_PUT_KEYS if k in col}
            col_name = clean_col.get("column_name")
            if col_name in COLUMN_VERBOSE_NAMES:
                target_vn = COLUMN_VERBOSE_NAMES[col_name]
                if clean_col.get("verbose_name") != target_vn:
                    clean_col["verbose_name"] = target_vn
                    needs_col_update = True
            updated_columns.append(clean_col)

        update_payload = {}
        if main_dttm_col and main_dttm_col in col_names and detail.get("main_dttm_col") != main_dttm_col:
            update_payload["main_dttm_col"] = main_dttm_col
        if needs_col_update:
            update_payload["columns"] = updated_columns

        if update_payload:
            self._raw_request(
                "PUT",
                f"/api/v1/dataset/{dataset_id}?override_columns=false",
                update_payload,
            )

        return {"id": dataset_id, "table_name": table_name}

    def upsert_chart(
        self,
        slice_name: str,
        viz_type: str,
        dataset_id: int,
        params: dict,
        query_context: dict = None,
        description: str = "",
        legacy_names: list[str] = None,
        existing_charts: dict = None,
    ) -> dict:
        full_params = {
            "datasource": f"{dataset_id}__table",
            "viz_type": viz_type,
            "time_range": "No filter",
            **params,
        }
        payload = {
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(full_params, ensure_ascii=False),
            "description": description,
        }
        if query_context is not None:
            payload["query_context"] = json.dumps(query_context, ensure_ascii=False)

        charts_by_name = (
            existing_charts
            if existing_charts is not None
            else {c.get("slice_name"): c for c in self.list_all("/api/v1/chart/")}
        )

        matched_entry = charts_by_name.get(slice_name)
        matched_old_key = None
        if matched_entry is None and legacy_names:
            for old_name in legacy_names:
                if old_name in charts_by_name:
                    matched_entry = charts_by_name[old_name]
                    matched_old_key = old_name
                    break

        if matched_entry is not None:
            chart_id = int(matched_entry["id"])
            self._raw_request("PUT", f"/api/v1/chart/{chart_id}", payload)
            if matched_old_key and existing_charts is not None:
                existing_charts.pop(matched_old_key, None)
        else:
            created = self._raw_request("POST", "/api/v1/chart/", payload)
            chart_id = int(created["id"])

        chart_detail = (
            self._raw_request("GET", f"/api/v1/chart/{chart_id}").get("result") or {}
        )
        chart_uuid = chart_detail.get("uuid") or str(
            uuid.uuid5(uuid.NAMESPACE_OID, f"superset-chart-{chart_id}")
        )
        info = {"id": chart_id, "uuid": chart_uuid, "slice_name": slice_name}
        if existing_charts is not None:
            existing_charts[slice_name] = info
        return info

    def upsert_dashboard(
        self,
        title: str,
        slug: str,
        position_json: dict,
        json_metadata: dict,
        existing_dashboards: dict = None,
    ) -> int:
        dash_by_slug = (
            existing_dashboards
            if existing_dashboards is not None
            else {d.get("slug"): d for d in self.list_all("/api/v1/dashboard/")}
        )

        payload = {
            "dashboard_title": title,
            "slug": slug,
            "position_json": json.dumps(position_json, ensure_ascii=False),
            "json_metadata": json.dumps(json_metadata, ensure_ascii=False),
            "published": True,
        }
        if slug in dash_by_slug:
            dash_id = int(dash_by_slug[slug]["id"])
            self._raw_request("PUT", f"/api/v1/dashboard/{dash_id}", payload)
            return dash_id

        created = self._raw_request("POST", "/api/v1/dashboard/", payload)
        dash_id = int(created["id"])
        if existing_dashboards is not None:
            existing_dashboards[slug] = {"id": dash_id, "slug": slug}
        return dash_id

    def link_slices_to_dashboard(self, dashboard_id: int, chart_ids: list[int]) -> None:
        """Ensure dashboard_slices many-to-many links are populated via chart dashboards field."""
        for cid in chart_ids:
            detail = self._raw_request("GET", f"/api/v1/chart/{cid}").get("result") or {}
            existing_dash_ids = [
                int(d["id"]) for d in (detail.get("dashboards") or []) if "id" in d
            ]
            if dashboard_id not in existing_dash_ids:
                self._raw_request(
                    "PUT",
                    f"/api/v1/chart/{cid}",
                    {"dashboards": sorted(set(existing_dash_ids + [dashboard_id]))},
                )


def sql_metric(sql_expr: str, label: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label,
    }


def make_table_query_context(
    dataset_id: int,
    columns: list[str],
    metrics: list[dict],
    orderby: list = None,
    row_limit: int = 200,
) -> dict:
    default_order = [[metrics[0], False]] if metrics else []
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [],
                "extras": {"having": "", "where": ""},
                "applied_time_extras": {},
                "columns": columns,
                "metrics": metrics,
                "orderby": orderby if orderby is not None else default_order,
                "annotation_layers": [],
                "row_limit": row_limit,
                "series_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
            }
        ],
        "result_format": "json",
        "result_type": "full",
    }


def make_pie_query_context(dataset_id: int, groupby: list[str], metric: dict) -> dict:
    return make_table_query_context(
        dataset_id=dataset_id,
        columns=groupby,
        metrics=[metric],
        orderby=[[metric, False]],
        row_limit=50,
    )


def make_big_number_query_context(dataset_id: int, metric: dict) -> dict:
    return make_table_query_context(
        dataset_id=dataset_id,
        columns=[],
        metrics=[metric],
        orderby=[],
        row_limit=1,
    )


def markdown_banner(text: str, height: int = 9) -> dict:
    return {"kind": "markdown", "code": text, "width": 12, "height": height}


def build_grid_position(title: str, rows_spec: list) -> dict:
    """
    Build Superset v2 dashboard position_json supporting both CHART tuples (chart_dict, width, height)
    and Markdown banner dicts {"kind": "markdown", "code": ..., "width": 12, "height": ...}.
    """
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {
            "id": "HEADER_ID",
            "type": "HEADER",
            "meta": {"text": title},
        },
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
    }

    for r_idx, row_items in enumerate(rows_spec, start=1):
        row_id = f"ROW-{r_idx}"
        pos["GRID_ID"]["children"].append(row_id)
        row_children = []
        for c_idx, entry in enumerate(row_items, start=1):
            if isinstance(entry, dict) and entry.get("kind") == "markdown":
                node_id = f"MARKDOWN-{r_idx}-{c_idx}"
                row_children.append(node_id)
                pos[node_id] = {
                    "type": "MARKDOWN",
                    "id": node_id,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row_id],
                    "meta": {
                        "code": entry["code"],
                        "width": entry.get("width", 12),
                        "height": entry.get("height", 9),
                    },
                }
            else:
                chart, width, height = entry
                node_id = f"CHART-{r_idx}-{c_idx}-{chart['id']}"
                row_children.append(node_id)
                pos[node_id] = {
                    "type": "CHART",
                    "id": node_id,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row_id],
                    "meta": {
                        "chartId": chart["id"],
                        "sliceName": chart["slice_name"],
                        "uuid": chart["uuid"],
                        "width": width,
                        "height": height,
                    },
                }
        pos[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    return pos


def build_json_metadata(chart_ids: list[int], label_colors: dict = None) -> dict:
    return {
        "color_scheme": "supersetColors",
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "label_colors": label_colors or {},
        "shared_label_colors": {},
        "cross_filters_enabled": False,
        "global_chart_configuration": {
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": chart_ids,
        },
    }


def provision_legacy_overview_dashboard(
    client: SupersetApiClient,
    ds: dict,
    existing_charts: dict,
    existing_dashboards: dict,
) -> tuple[int, list[int]]:
    """Việt hóa hoàn toàn Dashboard tổng quan K-Means ban đầu (covid19-kmeans-clusters)."""
    clus_id = ds["vw_weekly_covid_clusters"]["id"]

    m_weeks = sql_metric("COUNT(*)", "Số tuần ghi nhận")
    t_metrics = [
        m_weeks,
        sql_metric("ROUND(AVG(cases_per_100k), 1)", "Ca mắc TB / 100.000 dân"),
        sql_metric("ROUND(AVG(deaths_per_100k), 2)", "Tử vong TB / 100.000 dân"),
        sql_metric("ROUND(AVG(case_growth_pct), 1)", "Tăng trưởng ca TB (%)"),
    ]
    c_table = client.upsert_chart(
        slice_name="Bảng Tổng hợp 4 Nhóm Dịch tễ theo Cụm",
        legacy_names=["K-Means Clusters: Tổng hợp 4 nhóm dịch tễ"],
        viz_type="table",
        dataset_id=clus_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["cluster_id", "cluster_label"],
            "metrics": t_metrics,
            "order_desc": False,
            "adhoc_filters": [],
            "row_limit": 100,
            "show_cell_bars": True,
        },
        query_context=make_table_query_context(
            clus_id,
            ["cluster_id", "cluster_label"],
            t_metrics,
            orderby=[["cluster_id", True]],
            row_limit=100,
        ),
        existing_charts=existing_charts,
    )

    c_bubble = client.upsert_chart(
        slice_name="Tương quan Ca mắc & Tử vong theo Nhóm Dịch tễ",
        legacy_names=["Phân Cụm 2D: Ca Nhiễm vs Tử Vong (Bubble Chart)"],
        viz_type="bubble",
        dataset_id=clus_id,
        params={
            "entity": "country_name",
            "series": "cluster_label",
            "x": sql_metric("ROUND(AVG(cases_per_100k), 1)", "Ca mắc TB / 100.000 dân"),
            "y": sql_metric("ROUND(AVG(deaths_per_100k), 2)", "Tử vong TB / 100.000 dân"),
            "size": sql_metric("MAX(population)", "Quy mô dân số"),
            "limit": 200,
            "max_bubble_size": "40",
            "show_legend": True,
            "x_axis_label": "Số ca mắc trung bình / 100.000 dân",
            "y_axis_label": "Số ca tử vong trung bình / 100.000 dân",
            "adhoc_filters": [],
            "row_limit": 1000,
            "color_scheme": "supersetColors",
        },
        existing_charts=existing_charts,
    )

    c_pie = client.upsert_chart(
        slice_name="Tỷ lệ Phân bố 4 Nhóm Dịch tễ Toàn cầu",
        legacy_names=["Tỷ lệ Phân Bố 4 Cụm Dịch Tễ (Pie Chart)"],
        viz_type="pie",
        dataset_id=clus_id,
        params={
            "groupby": ["cluster_label"],
            "metric": m_weeks,
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "number_format": ",d",
            "adhoc_filters": [],
            "row_limit": 20,
            "color_scheme": "supersetColors",
        },
        query_context=make_pie_query_context(clus_id, ["cluster_label"], m_weeks),
        existing_charts=existing_charts,
    )

    c_bar = client.upsert_chart(
        slice_name="Đối sánh Mức Ca mắc & Tử vong giữa 4 Nhóm Dịch tễ",
        legacy_names=["So Sánh Ca Nhiễm & Tử Vong giữa 4 Cụm (Bar Chart)"],
        viz_type="dist_bar",
        dataset_id=clus_id,
        params={
            "groupby": ["cluster_label"],
            "columns": [],
            "metrics": [
                sql_metric("ROUND(AVG(cases_per_100k), 1)", "Ca mắc TB / 100.000 dân"),
                sql_metric("ROUND(AVG(deaths_per_100k) * 50, 1)", "Tử vong TB / 100.000 dân (nhân 50)"),
            ],
            "show_legend": True,
            "bar_stacked": False,
            "x_axis_label": "Nhãn cụm dịch tễ",
            "y_axis_label": "Tỷ lệ trên 100.000 dân",
            "adhoc_filters": [],
            "row_limit": 50,
            "color_scheme": "supersetColors",
        },
        existing_charts=existing_charts,
    )

    charts = [c_bubble, c_pie, c_bar, c_table]
    title = "Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu (4 Nhóm Xu hướng)"
    pos = build_grid_position(
        title=title,
        rows_spec=[
            [
                markdown_banner(
                    "### Tổng quan 4 Nhóm Xu hướng Dịch tễ COVID-19\n"
                    "Phân nhóm **56.477 quan sát tuần – quốc gia** từ dữ liệu OWID dựa trên tỷ lệ mắc mới, tử vong trên 100.000 dân và tốc độ tăng trưởng tuần.",
                    height=10,
                )
            ],
            [(c_bubble, 8, 54), (c_pie, 4, 54)],
            [(c_bar, 6, 50), (c_table, 6, 50)],
        ],
    )
    label_colors = {
        "Thấp – ổn định": "#10b981",
        "Trung bình – ổn định": "#f59e0b",
        "Cao – đang tăng": "#f97316",
        "Rất cao – ổn định": "#ef4444",
    }
    chart_ids = [c["id"] for c in charts]
    dash_id = client.upsert_dashboard(
        title=title,
        slug="covid19-kmeans-clusters",
        position_json=pos,
        json_metadata=build_json_metadata(chart_ids, label_colors),
        existing_dashboards=existing_dashboards,
    )
    client.link_slices_to_dashboard(dash_id, chart_ids)
    return dash_id, chart_ids


def provision_kmeans_dashboard(
    client: SupersetApiClient,
    ds: dict,
    existing_charts: dict,
    existing_dashboards: dict,
) -> tuple[int, list[int]]:
    eval_id = ds["vw_kmeans_evaluation"]["id"]
    prof_id = ds["vw_kmeans_cluster_profile"]["id"]
    clus_id = ds["vw_weekly_covid_clusters"]["id"]
    trans_id = ds["vw_cluster_transition"]["id"]
    dwell_id = ds["vw_cluster_dwell_time"]["id"]

    # 1. Elbow Chart (Inertia theo K)
    c_elbow = client.upsert_chart(
        slice_name="Độ phân tán nội cụm theo Số cụm K (Phương pháp Khuỷu tay)",
        legacy_names=["[K-Means] Biểu đồ Elbow: Inertia (WCSS) theo K"],
        viz_type="dist_bar",
        dataset_id=eval_id,
        params={
            "groupby": ["k"],
            "columns": [],
            "metrics": [sql_metric("ROUND(AVG(inertia)::numeric, 0)", "Tổng phương sai nội cụm")],
            "order_bars": True,
            "show_legend": True,
            "show_bar_value": True,
            "bar_stacked": False,
            "y_axis_format": ",d",
            "x_axis_label": "Số cụm thử nghiệm (K = 2 đến 8)",
            "y_axis_label": "Độ phân tán nội cụm (càng thấp càng gom chặt)",
            "adhoc_filters": [],
            "row_limit": 50,
            "color_scheme": "supersetColors",
        },
        description="Đánh giá điểm gãy khuỷu tay trên dải số cụm K từ 2 đến 8.",
        existing_charts=existing_charts,
    )

    # 2. Silhouette & Davies-Bouldin theo K
    c_sil = client.upsert_chart(
        slice_name="Độ tách biệt Cụm theo K (Hệ số Silhouette & Davies–Bouldin)",
        legacy_names=["[K-Means] Chỉ số Silhouette & Davies-Bouldin theo K"],
        viz_type="dist_bar",
        dataset_id=eval_id,
        params={
            "groupby": ["k"],
            "columns": [],
            "metrics": [
                sql_metric("ROUND(AVG(silhouette)::numeric, 4)", "Hệ số Silhouette (cao là tốt)"),
                sql_metric("ROUND(AVG(davies_bouldin)::numeric, 4)", "Chỉ số Davies–Bouldin (thấp là tốt)"),
            ],
            "order_bars": True,
            "show_legend": True,
            "show_bar_value": True,
            "bar_stacked": False,
            "x_axis_label": "Số cụm thử nghiệm (K = 2 đến 8)",
            "y_axis_label": "Giá trị chỉ số đánh giá",
            "adhoc_filters": [],
            "row_limit": 50,
            "color_scheme": "supersetColors",
        },
        description="Kiểm định định lượng chất lượng tách cụm để chọn số cụm K có căn cứ.",
        existing_charts=existing_charts,
    )

    # 3. Scatter / Bubble PCA 2D tô màu theo cluster_label
    c_pca = client.upsert_chart(
        slice_name="Bản đồ Phân bố các Cụm Dịch tễ trên Không gian 2 Chiều (PCA)",
        legacy_names=["[K-Means] Không gian PCA 2D theo Nhãn Cụm Dịch Tễ"],
        viz_type="bubble",
        dataset_id=clus_id,
        params={
            "series": "cluster_label",
            "entity": "country_name",
            "x": sql_metric("ROUND(AVG(pca_1)::numeric, 3)", "Trục thành phần 1 (Mức ca mắc & tử vong)"),
            "y": sql_metric("ROUND(AVG(pca_2)::numeric, 3)", "Trục thành phần 2 (Đà tăng trưởng tuần)"),
            "size": sql_metric("ROUND(AVG(cases_per_100k)::numeric + 1, 1)", "Ca mắc TB / 100.000 dân"),
            "max_bubble_size": "35",
            "limit": 500,
            "show_legend": True,
            "x_axis_label": "Trục phân cụm thứ nhất (Mức độ nghiêm trọng)",
            "y_axis_label": "Trục phân cụm thứ hai (Tốc độ biến động ca mắc)",
            "adhoc_filters": [],
            "row_limit": 1000,
            "color_scheme": "supersetColors",
        },
        description="Giảm chiều dữ liệu từ 3 đặc trưng chuẩn hóa xuống mặt phẳng 2 chiều để quan sát ranh giới cụm.",
        existing_charts=existing_charts,
    )

    # 4. Pie Donut tỉ trọng cụm theo nhãn tự sinh
    m_members = sql_metric("SUM(member_count)", "Số tuần – quốc gia")
    c_pie = client.upsert_chart(
        slice_name="Cơ cấu Tỉ trọng Số tuần – Quốc gia theo Nhãn Cụm",
        legacy_names=["[K-Means] Tỉ trọng Quan sát theo Nhãn Cụm"],
        viz_type="pie",
        dataset_id=prof_id,
        params={
            "groupby": ["cluster_label"],
            "metric": m_members,
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "labels_outside": True,
            "number_format": ",d",
            "adhoc_filters": [],
            "row_limit": 20,
            "color_scheme": "supersetColors",
        },
        query_context=make_pie_query_context(prof_id, ["cluster_label"], m_members),
        description="Cơ cấu phân bố số lượng quan sát tuần – quốc gia trong từng nhóm dịch tễ.",
        existing_charts=existing_charts,
    )

    # 5. Bảng Hồ sơ cụm chi tiết
    prof_metrics = [
        sql_metric("MAX(member_count)", "Số tuần – QG"),
        sql_metric("ROUND((MAX(member_share) * 100)::numeric, 1)", "Tỉ trọng (%)"),
        sql_metric("ROUND(MAX(avg_cases_per_100k)::numeric, 1)", "Ca mắc TB / 100k"),
        sql_metric("ROUND(MAX(median_cases_per_100k)::numeric, 1)", "Ca mắc trung vị / 100k"),
        sql_metric("ROUND(MAX(avg_deaths_per_100k)::numeric, 2)", "Tử vong TB / 100k"),
        sql_metric("ROUND(MAX(avg_silhouette)::numeric, 3)", "Độ gắn kết cụm"),
    ]
    c_prof_table = client.upsert_chart(
        slice_name="Bảng Hồ sơ Thống kê & Nhãn Dịch tễ Tự sinh của Từng Cụm",
        legacy_names=["[K-Means] Bảng Hồ Sơ Cụm & Nhãn Dịch Tễ Tự Sinh"],
        viz_type="table",
        dataset_id=prof_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["cluster_id", "cluster_label"],
            "metrics": prof_metrics,
            "order_desc": False,
            "row_limit": 50,
            "adhoc_filters": [],
            "show_cell_bars": True,
            "color_pn": True,
        },
        query_context=make_table_query_context(
            prof_id,
            ["cluster_id", "cluster_label"],
            prof_metrics,
            orderby=[["cluster_id", True]],
            row_limit=50,
        ),
        description="Tổng hợp trung bình, trung vị ca mắc/tử vong và độ gắn kết Silhouette riêng từng cụm.",
        existing_charts=existing_charts,
    )

    # 6. Chuỗi thời gian Ca mắc TB/100k theo nhãn cụm
    c_trend = client.upsert_chart(
        slice_name="Diễn biến Ca mắc Trung bình (/100.000 dân) theo Cụm qua các Năm",
        legacy_names=["[K-Means] Diễn biến Ca mắc TB/100k theo Cụm qua Thời gian"],
        viz_type="line",
        dataset_id=clus_id,
        params={
            "granularity_sqla": "week_start",
            "time_grain_sqla": "P1W",
            "time_range": "No filter",
            "groupby": ["cluster_label"],
            "metrics": [sql_metric("ROUND(AVG(cases_per_100k)::numeric, 1)", "Ca mắc TB / 100.000 dân")],
            "show_legend": True,
            "rich_tooltip": True,
            "x_axis_label": "Thời gian (theo tuần)",
            "y_axis_label": "Ca mắc mới / 100.000 dân",
            "x_axis_format": "%Y-%m",
            "y_axis_format": ",.1f",
            "adhoc_filters": [],
            "row_limit": 10000,
            "color_scheme": "supersetColors",
        },
        description="Xu hướng tỷ lệ ca mắc mới trên 100.000 dân của từng nhóm dịch tễ qua các đợt bùng phát.",
        existing_charts=existing_charts,
    )

    # 7. Ma trận Xác suất Chuyển dịch Cụm Markov bậc 1 (%)
    c_markov = client.upsert_chart(
        slice_name="Ma trận Xác suất Chuyển dịch Cụm sang Tuần Kế tiếp (Markov %)",
        viz_type="dist_bar",
        dataset_id=trans_id,
        params={
            "groupby": ["from_cluster_label"],
            "columns": ["to_cluster_label"],
            "metrics": [sql_metric("ROUND(MAX(transition_prob_pct)::numeric, 2)", "Xác suất chuyển cụm (%)")],
            "order_bars": False,
            "show_legend": True,
            "show_bar_value": True,
            "bar_stacked": True,
            "x_axis_label": "Cụm dịch tễ ở tuần hiện tại (t)",
            "y_axis_label": "Xác suất chuyển sang cụm ở tuần t+1 (%)",
            "adhoc_filters": [],
            "row_limit": 50,
            "color_scheme": "supersetColors",
        },
        description="Xác suất một quốc gia giữ nguyên cụm, leo thang dịch hoặc hạ nhiệt vào tuần kế tiếp.",
        existing_charts=existing_charts,
    )

    # 8. Bảng Thời gian Lưu trú Liên tục (Dwell Time) & Xác suất Duy trì Cụm
    dwell_metrics = [
        sql_metric("MAX(episode_count)", "Số đợt lưu trú"),
        sql_metric("ROUND(MAX(avg_dwell_weeks)::numeric, 2)", "Số tuần lưu trú TB / đợt"),
        sql_metric("ROUND(MAX(median_dwell_weeks)::numeric, 1)", "Số tuần trung vị / đợt"),
        sql_metric("MAX(max_dwell_weeks)", "Đợt kéo dài lâu nhất (tuần)"),
        sql_metric("ROUND(MAX(retention_prob_pct)::numeric, 2)", "Xác suất duy trì cụm (%)"),
    ]
    c_dwell = client.upsert_chart(
        slice_name="Bảng Thời gian Lưu trú Liên tục (Dwell Time) & Độ Bền từng Cụm",
        viz_type="table",
        dataset_id=dwell_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["cluster_id", "cluster_label"],
            "metrics": dwell_metrics,
            "order_desc": False,
            "row_limit": 20,
            "adhoc_filters": [],
            "show_cell_bars": True,
        },
        query_context=make_table_query_context(
            dwell_id,
            ["cluster_id", "cluster_label"],
            dwell_metrics,
            orderby=[["cluster_id", True]],
            row_limit=20,
        ),
        description="Thống kê số tuần liên tiếp trung bình một quốc gia lưu lại trong từng vùng dịch trước khi chuyển cụm.",
        existing_charts=existing_charts,
    )

    charts = [c_elbow, c_sil, c_pca, c_pie, c_prof_table, c_trend, c_markov, c_dwell]
    title = "Phân tích Phân cụm Dịch tễ COVID-19 — Đánh giá Số cụm K, PCA & Chuyển cụm Markov"
    pos = build_grid_position(
        title=title,
        rows_spec=[
            [
                markdown_banner(
                    "### Phần 1: Căn cứ lựa chọn số cụm K (Kiểm định từ K = 2 đến K = 8)\n"
                    "So sánh độ phân tán nội cụm (**Phương pháp Khuỷu tay**) và độ tách biệt giữa các cụm (**Silhouette** càng cao càng tốt, **Davies–Bouldin** càng thấp càng tốt).",
                    height=10,
                )
            ],
            [(c_elbow, 6, 48), (c_sil, 6, 48)],
            [
                markdown_banner(
                    "### Phần 2: Hình thái phân tách cụm trên mặt phẳng 2 chiều & Hồ sơ nghiệp vụ tự sinh\n"
                    "Mỗi cụm được hệ thống tự động đặt nhãn tiếng Việt dựa trên thứ hạng tỷ lệ ca mắc và hướng dịch chuyển của tâm cụm so với tuần liền trước.",
                    height=10,
                )
            ],
            [(c_pca, 8, 56), (c_pie, 4, 56)],
            [(c_prof_table, 6, 50), (c_trend, 6, 50)],
            [
                markdown_banner(
                    "### Phần 3: Khai phá Chuỗi Chuyển dịch Cụm (Markov Transition Matrix & Thời gian Lưu trú Dwell Time)\n"
                    "Đo lường xác suất chuyển trạng thái $P(\\text{Cụm}_{t+1} \\mid \\text{Cụm}_t)$ và số tuần trung bình một quốc gia lưu lại trong từng vùng dịch.",
                    height=10,
                )
            ],
            [(c_markov, 6, 52), (c_dwell, 6, 52)],
        ],
    )
    label_colors = {
        "Thấp – ổn định": "#10b981",
        "Trung bình – ổn định": "#f59e0b",
        "Trung bình – đang giảm": "#38bdf8",
        "Cao – đang tăng": "#f97316",
        "Rất cao – ổn định": "#ef4444",
        "Rất cao – đang tăng": "#dc2626",
    }
    chart_ids = [c["id"] for c in charts]
    dash_id = client.upsert_dashboard(
        title=title,
        slug="covid19-kmeans-mining",
        position_json=pos,
        json_metadata=build_json_metadata(chart_ids, label_colors),
        existing_dashboards=existing_dashboards,
    )
    client.link_slices_to_dashboard(dash_id, chart_ids)
    return dash_id, chart_ids


def provision_early_warning_dashboard(
    client: SupersetApiClient,
    ds: dict,
    existing_charts: dict,
    existing_dashboards: dict,
) -> tuple[int, list[int]]:
    latest_id = ds["vw_early_warning_latest"]["id"]
    eval_id = ds["vw_early_warning_evaluation"]["id"]
    imp_id = ds["vw_early_warning_feature_importance"]["id"]

    # 1. World Map xác suất bùng phát dịch
    c_map = client.upsert_chart(
        slice_name="Bản đồ Thế giới: Xác suất Bùng phát Dịch trong 2 Tuần tới",
        legacy_names=["[Early Warning] Bản đồ Nguy cơ Bùng phát Dịch 2 Tuần tới"],
        viz_type="world_map",
        dataset_id=latest_id,
        params={
            "entity": "country_code",
            "country_fieldtype": "cca3",
            "metric": sql_metric(
                "ROUND((MAX(surge_probability) * 100)::numeric, 1)",
                "Xác suất bùng phát (%)",
            ),
            "secondary_metric": sql_metric(
                "ROUND(MAX(cases_per_100k)::numeric, 1)",
                "Ca mắc hiện tại / 100.000 dân",
            ),
            "show_bubbles": True,
            "max_bubble_size": "25",
            "adhoc_filters": [],
            "row_limit": 500,
            "color_scheme": "supersetColors",
        },
        description="Bản đồ dự báo nguy cơ ca mắc tăng từ 50% trở lên sau 2 tuần tại từng quốc gia.",
        existing_charts=existing_charts,
    )

    # 2. Pie Donut phân bố mức cảnh báo (Nguy cơ cao / Cần theo dõi / Nguy cơ thấp)
    m_countries = sql_metric("COUNT(*)", "Số quốc gia")
    c_alert_pie = client.upsert_chart(
        slice_name="Tỷ lệ Quốc gia theo Mức Cảnh báo Sớm",
        legacy_names=["[Early Warning] Phân bố Quốc gia theo Mức Cảnh báo"],
        viz_type="pie",
        dataset_id=latest_id,
        params={
            "groupby": ["alert_level_vi"],
            "metric": m_countries,
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "number_format": ",d",
            "adhoc_filters": [],
            "row_limit": 10,
            "color_scheme": "supersetColors",
        },
        query_context=make_pie_query_context(latest_id, ["alert_level_vi"], m_countries),
        description="Phân bổ các quốc gia theo 3 ngưỡng cảnh báo: Nguy cơ cao, Cần theo dõi và Nguy cơ thấp.",
        existing_charts=existing_charts,
    )

    # 3. So sánh hiệu năng 3 mô hình bằng tên tiếng Việt (model_name_vi)
    c_model_cmp = client.upsert_chart(
        slice_name="Đối sánh Độ chính xác của 3 Mô hình Dự báo trên Tập Kiểm thử",
        legacy_names=["[Early Warning] So sánh Hiệu năng 3 Mô hình trên Tập Test"],
        viz_type="dist_bar",
        dataset_id=eval_id,
        params={
            "groupby": ["model_name_vi"],
            "columns": [],
            "metrics": [
                sql_metric("ROUND(MAX(roc_auc)::numeric, 3)", "Khả năng phân loại (ROC-AUC)"),
                sql_metric("ROUND(MAX(pr_auc)::numeric, 3)", "Độ nhạy lớp hiếm (PR-AUC)"),
                sql_metric("ROUND(MAX(f1_score)::numeric, 3)", "Điểm cân bằng F1"),
                sql_metric("ROUND(MAX(recall_score)::numeric, 3)", "Tỷ lệ bắt trúng ổ dịch (Recall)"),
                sql_metric("ROUND(MAX(precision_score)::numeric, 3)", "Độ chuẩn xác cảnh báo (Precision)"),
            ],
            "show_legend": True,
            "show_bar_value": True,
            "bar_stacked": False,
            "x_axis_label": "Mô hình dự báo",
            "y_axis_label": "Điểm đánh giá (0 đến 1)",
            "adhoc_filters": [],
            "row_limit": 20,
            "color_scheme": "supersetColors",
        },
        description="So sánh XGBoost, Hồi quy Logistic và Luật cơ sở trên tập kiểm thử tách biệt thời gian.",
        existing_charts=existing_charts,
    )

    # 4. Feature Importance tiếng Việt (feature_name_vi)
    c_feat_imp = client.upsert_chart(
        slice_name="10 Yếu tố Ảnh hưởng Lớn nhất đến Nguy cơ Bùng phát Dịch",
        legacy_names=["[Early Warning] Top Đặc trưng Quan trọng nhất (XGBoost Gain %)"],
        viz_type="dist_bar",
        dataset_id=imp_id,
        params={
            "groupby": ["feature_name_vi"],
            "columns": [],
            "metrics": [
                sql_metric("ROUND((MAX(importance) * 100)::numeric, 2)", "Mức độ ảnh hưởng (%)")
            ],
            "order_bars": False,
            "show_legend": False,
            "show_bar_value": True,
            "bar_stacked": False,
            "x_axis_label": "Đặc trưng dịch tễ đầu vào",
            "y_axis_label": "Tỷ trọng đóng góp (%)",
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "importance_rank",
                    "operator": "<=",
                    "comparator": "10",
                    "clause": "WHERE",
                    "sqlExpression": None,
                    "isExtra": False,
                    "isNew": False,
                }
            ],
            "row_limit": 10,
            "color_scheme": "supersetColors",
        },
        description="Xếp hạng mức độ đóng góp của các biến trễ, đà tăng trưởng, tử vong và vắc-xin trong XGBoost.",
        existing_charts=existing_charts,
    )

    # 5. Bảng xếp hạng cảnh báo sớm theo quốc gia
    m_prob = sql_metric("ROUND((MAX(surge_probability) * 100)::numeric, 1)", "Xác suất bùng phát (%)")
    latest_metrics = [
        m_prob,
        sql_metric("ROUND(MAX(cases_per_100k)::numeric, 1)", "Ca mắc hiện tại / 100k"),
        sql_metric("ROUND(MAX(COALESCE(case_growth_pct, 0))::numeric, 1)", "Tăng trưởng tuần (%)"),
        sql_metric("ROUND((MAX(decision_threshold) * 100)::numeric, 1)", "Ngưỡng báo động (%)"),
    ]
    c_alert_table = client.upsert_chart(
        slice_name="Bảng Xếp hạng Cảnh báo Sớm theo Quốc gia (Kỳ Dự báo Mới nhất)",
        legacy_names=["[Early Warning] Bảng Xếp hạng Quốc gia Nguy cơ Bùng phát Mới nhất"],
        viz_type="table",
        dataset_id=latest_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["country_code", "country_name", "week_start", "alert_level_vi"],
            "metrics": latest_metrics,
            "order_desc": True,
            "row_limit": 100,
            "adhoc_filters": [],
            "show_cell_bars": True,
        },
        query_context=make_table_query_context(
            latest_id,
            ["country_code", "country_name", "week_start", "alert_level_vi"],
            latest_metrics,
            orderby=[[m_prob, False]],
            row_limit=100,
        ),
        description="Danh sách các quốc gia sắp xếp theo xác suất bùng phát dịch trong 2 tuần tới từ cao xuống thấp.",
        existing_charts=existing_charts,
    )

    charts = [c_map, c_alert_pie, c_model_cmp, c_feat_imp, c_alert_table]
    title = "Hệ thống Cảnh báo Sớm Nguy cơ Bùng phát Dịch COVID-19 (Dự báo trước 2 Tuần)"
    pos = build_grid_position(
        title=title,
        rows_spec=[
            [
                markdown_banner(
                    "### Phần 1: Bản đồ Giám sát & Phân tầng Nguy cơ Bùng phát Toàn cầu\n"
                    "Mô hình học máy **XGBoost** dự báo xác suất tỷ lệ ca mắc tăng tối thiểu **1,5 lần** sau **2 tuần** (và vượt ngưỡng 10 ca/100.000 dân).",
                    height=10,
                )
            ],
            [(c_map, 8, 56), (c_alert_pie, 4, 56)],
            [
                markdown_banner(
                    "### Phần 2: Kiểm định Mô hình Chống Rò rỉ Thời gian & Giải thích Yếu tố Dự báo\n"
                    "Đánh giá độc lập trên **11.006 quan sát kiểm thử** (có khoảng cách 2 tuần ngăn rò rỉ nhãn tương lai) và xếp hạng các yếu tố thúc đẩy dịch bùng phát.",
                    height=10,
                )
            ],
            [(c_model_cmp, 6, 50), (c_feat_imp, 6, 50)],
            [(c_alert_table, 12, 56)],
        ],
    )
    label_colors = {
        "Nguy cơ cao": "#ef4444",
        "Cần theo dõi": "#f59e0b",
        "Nguy cơ thấp": "#10b981",
        "HIGH": "#ef4444",
        "WATCH": "#f59e0b",
        "LOW": "#10b981",
        "XGBoost (Cây tăng cường)": "#0284c7",
        "Hồi quy Logistic": "#8b5cf6",
        "Luật cơ sở (Tăng ≥ 25%)": "#64748b",
    }
    chart_ids = [c["id"] for c in charts]
    dash_id = client.upsert_dashboard(
        title=title,
        slug="covid19-early-warning",
        position_json=pos,
        json_metadata=build_json_metadata(chart_ids, label_colors),
        existing_dashboards=existing_dashboards,
    )
    client.link_slices_to_dashboard(dash_id, chart_ids)
    return dash_id, chart_ids


def provision_cdc_patients_dashboard(
    client: SupersetApiClient,
    ds: dict,
    existing_charts: dict,
    existing_dashboards: dict,
) -> tuple[int, list[int]]:
    pat_id = ds["vw_dashboard_patients"]["id"]
    vax_id = ds["vw_dashboard_vaccinations"]["id"]
    vax_pat_id = ds["vw_vaccination_by_patient"]["id"]
    rules_id = ds["vw_patient_association_rules"]["id"]

    # 1. Big Number: Tổng bệnh nhân ẩn danh
    m_total_pat = sql_metric("SUM(patient_count)", "Tổng số bệnh nhân")
    c_kpi_pat = client.upsert_chart(
        slice_name="Tổng số Hồ sơ Bệnh nhân Đã Đồng bộ Ẩn danh",
        legacy_names=["[CDC] Tổng Hồ sơ Bệnh nhân Đã Ẩn danh trong DWH"],
        viz_type="big_number_total",
        dataset_id=pat_id,
        params={
            "metric": m_total_pat,
            "subheader": "Đồng bộ tự động từ MySQL sang Kho dữ liệu (đã lược bỏ họ tên & mã định danh)",
            "y_axis_format": ",d",
            "adhoc_filters": [],
        },
        query_context=make_big_number_query_context(pat_id, m_total_pat),
        description="Tổng số bệnh nhân đã được ẩn danh trong bảng dwh.dim_patient.",
        existing_charts=existing_charts,
    )

    # 2. Big Number: Tổng mũi tiêm nội bộ
    m_total_vax = sql_metric("SUM(vaccination_count)", "Tổng số mũi tiêm")
    c_kpi_vax = client.upsert_chart(
        slice_name="Tổng số Mũi tiêm Đã Ghi nhận Thời gian thực",
        legacy_names=["[CDC] Tổng Lượt Tiêm chủng Đã Đồng bộ Realtime"],
        viz_type="big_number_total",
        dataset_id=vax_id,
        params={
            "metric": m_total_vax,
            "subheader": "Luồng đồng bộ biến động dữ liệu tức thì qua Debezium & Kafka",
            "y_axis_format": ",d",
            "adhoc_filters": [],
        },
        query_context=make_big_number_query_context(vax_id, m_total_vax),
        description="Tổng số lượt tiêm chủng ghi nhận trong bảng dwh.fact_vaccination.",
        existing_charts=existing_charts,
    )

    # 3. Phân bố tình trạng sức khỏe theo nhóm tuổi (tiếng Việt 100%)
    c_age_status = client.upsert_chart(
        slice_name="Phân bố Tình trạng Sức khỏe theo Nhóm Tuổi",
        legacy_names=["[CDC] Tình trạng Nhiễm bệnh theo Nhóm Tuổi"],
        viz_type="dist_bar",
        dataset_id=pat_id,
        params={
            "groupby": ["age_group_vi"],
            "columns": ["health_status_vi"],
            "metrics": [sql_metric("SUM(patient_count)", "Số bệnh nhân")],
            "order_bars": True,
            "show_legend": True,
            "bar_stacked": True,
            "x_axis_label": "Nhóm tuổi",
            "y_axis_label": "Số lượng bệnh nhân",
            "y_axis_format": ",d",
            "adhoc_filters": [],
            "row_limit": 100,
            "color_scheme": "supersetColors",
        },
        description="Cơ cấu ca Dương tính COVID-19, Nghi nhiễm, Đã khỏi bệnh và Bình thường theo từng nhóm tuổi.",
        existing_charts=existing_charts,
    )

    # 4. Tỷ lệ bệnh nền của bệnh nhân (tiếng Việt 100%)
    m_pat_cnt = sql_metric("SUM(patient_count)", "Số bệnh nhân")
    c_comorb_pie = client.upsert_chart(
        slice_name="Cơ cấu Nhóm Bệnh nền của Bệnh nhân",
        legacy_names=["[CDC] Cơ cấu Bệnh nền của Bệnh nhân"],
        viz_type="pie",
        dataset_id=pat_id,
        params={
            "groupby": ["comorbidity_vi"],
            "metric": m_pat_cnt,
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "number_format": ",d",
            "adhoc_filters": [],
            "row_limit": 20,
            "color_scheme": "supersetColors",
        },
        query_context=make_pie_query_context(pat_id, ["comorbidity_vi"], m_pat_cnt),
        description="Tỷ lệ bệnh nhân mắc Tăng huyết áp, Tiểu đường, Tim mạch, Hô hấp mạn tính, Béo phì hoặc Không có bệnh nền.",
        existing_charts=existing_charts,
    )

    # 5. Số mũi tiêm theo loại vaccine & nhóm tuổi (tiếng Việt 100%)
    c_vax_by_age = client.upsert_chart(
        slice_name="Số Lượt Tiêm theo Loại Vắc-xin & Nhóm Tuổi",
        legacy_names=["[CDC] Phân bổ Mũi tiêm theo Loại Vaccine & Nhóm Tuổi"],
        viz_type="dist_bar",
        dataset_id=vax_pat_id,
        params={
            "groupby": ["vaccine_type"],
            "columns": ["age_group_vi"],
            "metrics": [sql_metric("SUM(vaccination_count)", "Số lượt tiêm")],
            "show_legend": True,
            "bar_stacked": False,
            "x_axis_label": "Loại vắc-xin",
            "y_axis_label": "Số mũi đã tiêm",
            "y_axis_format": ",d",
            "adhoc_filters": [],
            "row_limit": 100,
            "color_scheme": "supersetColors",
        },
        description="Phân tích số mũi tiêm theo từng loại vắc-xin và độ tuổi người tiêm.",
        existing_charts=existing_charts,
    )

    # 6. Bảng thống kê theo Tỉnh/Thành phố
    prov_metrics = [
        sql_metric("SUM(patient_count)", "Tổng số bệnh nhân"),
        sql_metric(
            "SUM(CASE WHEN health_status = 'CONFIRMED_COVID' THEN patient_count ELSE 0 END)",
            "Ca dương tính COVID-19",
        ),
        sql_metric(
            "SUM(CASE WHEN comorbidity <> 'None' THEN patient_count ELSE 0 END)",
            "Số người có bệnh nền",
        ),
    ]
    c_prov_table = client.upsert_chart(
        slice_name="Bảng Thống kê Bệnh nhân, Ca Dương tính & Bệnh nền theo Tỉnh/Thành",
        legacy_names=["[CDC] Thống kê Bệnh nhân & Bệnh nền theo Tỉnh/Thành"],
        viz_type="table",
        dataset_id=pat_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["province"],
            "metrics": prov_metrics,
            "order_desc": True,
            "row_limit": 50,
            "adhoc_filters": [],
            "show_cell_bars": True,
        },
        query_context=make_table_query_context(
            pat_id,
            ["province"],
            prov_metrics,
            orderby=[[prov_metrics[0], False]],
            row_limit=50,
        ),
        description="Thống kê chi tiết theo từng địa phương từ nguồn dữ liệu vận hành nội bộ.",
        existing_charts=existing_charts,
    )

    # 7. Biểu đồ Top 10 Luật kết hợp có Độ nâng (Lift) cao nhất
    c_rules_bar = client.upsert_chart(
        slice_name="Top 10 Luật Kết hợp Dịch tễ có Độ nâng (Lift) Cao nhất",
        viz_type="dist_bar",
        dataset_id=rules_id,
        params={
            "groupby": ["rule_text_vi"],
            "columns": [],
            "metrics": [
                sql_metric("ROUND(MAX(lift)::numeric, 2)", "Độ nâng (Lift)"),
                sql_metric("ROUND(MAX(confidence)::numeric, 2)", "Độ tin cậy (0 – 1)"),
            ],
            "order_bars": False,
            "show_legend": True,
            "show_bar_value": True,
            "bar_stacked": False,
            "x_axis_label": "Luật kết hợp {Điều kiện} => {Kết cục}",
            "y_axis_label": "Hệ số Lift & Confidence",
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "rule_id",
                    "operator": "<=",
                    "comparator": "10",
                    "clause": "WHERE",
                    "sqlExpression": None,
                    "isExtra": False,
                    "isNew": False,
                }
            ],
            "row_limit": 10,
            "color_scheme": "supersetColors",
        },
        description="Xếp hạng các quy luật kết hợp mạnh nhất giữa tuổi, bệnh nền, số mũi tiêm và tình trạng nhiễm COVID-19.",
        existing_charts=existing_charts,
    )

    # 8. Bảng Chi tiết Luật kết hợp Bệnh nhân & Tiêm chủng (Apriori / FP-Growth)
    m_lift = sql_metric("ROUND(MAX(lift)::numeric, 2)", "Độ nâng (Lift)")
    rule_metrics = [
        m_lift,
        sql_metric("ROUND(MAX(confidence_pct)::numeric, 1)", "Độ tin cậy (%)"),
        sql_metric("ROUND(MAX(support_pct)::numeric, 2)", "Độ phổ biến (%)"),
        sql_metric("MAX(support_count)", "Số bệnh nhân khớp"),
        sql_metric("ROUND(MAX(conviction)::numeric, 2)", "Độ thuyết phục"),
    ]
    c_rules_table = client.upsert_chart(
        slice_name="Bảng Khai phá Luật Kết hợp Bệnh nhân & Tiêm chủng (Apriori / FP-Growth)",
        viz_type="table",
        dataset_id=rules_id,
        params={
            "query_mode": "aggregate",
            "groupby": ["rule_id", "antecedents", "target_category_vi"],
            "metrics": rule_metrics,
            "order_desc": True,
            "row_limit": 40,
            "adhoc_filters": [],
            "show_cell_bars": True,
        },
        query_context=make_table_query_context(
            rules_id,
            ["rule_id", "antecedents", "target_category_vi"],
            rule_metrics,
            orderby=[[m_lift, False]],
            row_limit=40,
        ),
        description="Danh sách các luật kết hợp khai phá từ hồ sơ bệnh nhân ẩn danh và lịch sử mũi tiêm.",
        existing_charts=existing_charts,
    )

    charts = [
        c_kpi_pat,
        c_kpi_vax,
        c_age_status,
        c_comorb_pie,
        c_vax_by_age,
        c_prov_table,
        c_rules_bar,
        c_rules_table,
    ]
    title = "Giám sát Thời gian thực & Khai phá Luật Kết hợp — Bệnh nhân Ẩn danh & Tiêm chủng"
    pos = build_grid_position(
        title=title,
        rows_spec=[
            [
                markdown_banner(
                    "### Phần 1: Đồng bộ Biến động Dữ liệu Thời gian thực (MySQL → Debezium → Kafka → PostgreSQL)\n"
                    "Mọi hồ sơ bệnh nhân phát sinh mới đều được tự động loại bỏ thông tin định danh cá nhân (**họ tên, mã công dân**) trước khi đưa vào Kho dữ liệu phân tích.",
                    height=10,
                )
            ],
            [(c_kpi_pat, 6, 32), (c_kpi_vax, 6, 32)],
            [(c_age_status, 7, 50), (c_comorb_pie, 5, 50)],
            [(c_vax_by_age, 6, 50), (c_prov_table, 6, 50)],
            [
                markdown_banner(
                    "### Phần 2: Khai phá Luật Kết hợp Dịch tễ (Association Rule Mining — Apriori / FP-Growth)\n"
                    "Phát hiện các tổ hợp **{Nhóm tuổi + Bệnh nền + Số mũi tiêm / Loại vắc-xin} ⇒ {Tình trạng COVID-19}** có **Độ tin cậy (Confidence)** và **Độ nâng (Lift)** cao vượt trội.",
                    height=10,
                )
            ],
            [(c_rules_bar, 5, 56), (c_rules_table, 7, 56)],
        ],
    )
    label_colors = {
        "Dương tính COVID-19": "#ef4444",
        "Nghi nhiễm": "#f59e0b",
        "Đã khỏi bệnh": "#38bdf8",
        "Bình thường": "#10b981",
        "Không có bệnh nền": "#10b981",
        "Tăng huyết áp": "#f97316",
        "Tiểu đường": "#ef4444",
        "Bệnh tim mạch": "#dc2626",
        "Bệnh hô hấp mạn tính": "#8b5cf6",
        "Béo phì": "#eab308",
    }
    chart_ids = [c["id"] for c in charts]
    dash_id = client.upsert_dashboard(
        title=title,
        slug="covid19-cdc-patients-vaccination",
        position_json=pos,
        json_metadata=build_json_metadata(chart_ids, label_colors),
        existing_dashboards=existing_dashboards,
    )
    client.link_slices_to_dashboard(dash_id, chart_ids)
    return dash_id, chart_ids


def main():
    parser = argparse.ArgumentParser(
        description="Tự động khởi tạo & Việt hóa toàn bộ Datasets, Charts và Dashboards trên Apache Superset."
    )
    parser.add_argument("--url", default=DEFAULT_SUPERSET_URL, help="Địa chỉ Superset (mặc định: http://localhost:8089)")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help="Tài khoản admin Superset")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Mật khẩu admin Superset")
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME, help="Tên kết nối Database DWH trên Superset")
    args = parser.parse_args()

    print("=" * 88)
    print("  KHỞI TẠO & VIỆT HÓA TỰ ĐỘNG DATASETS, BIỂU ĐỒ & DASHBOARDS TRÊN APACHE SUPERSET")
    print("=" * 88)

    client = SupersetApiClient(args.url, args.username, args.password)
    print(f"[1/4] Đăng nhập Superset REST API tại {args.url} ...")
    client.authenticate()
    db_id = client.ensure_database(args.db_name, DEFAULT_DWH_URI)
    print(f"      -> Đã xác nhận kết nối Database '{args.db_name}' (id={db_id})")

    print(f"[2/4] Đồng bộ cột & gán nhãn tiếng Việt cho {len(ALL_DWH_VIEWS)} Dataset Views trong schema 'dwh'...")
    existing_ds = {
        (item.get("schema"), item.get("table_name")): item
        for item in client.list_all("/api/v1/dataset/")
        if int((item.get("database") or {}).get("id", db_id)) == db_id
    }
    datasets = {}
    for view_name, dttm_col in ALL_DWH_VIEWS:
        info = client.ensure_dataset(
            db_id,
            "dwh",
            view_name,
            main_dttm_col=dttm_col,
            existing_datasets=existing_ds,
        )
        datasets[view_name] = info
        print(f"      -> Dataset dwh.{view_name:<38} (id={info['id']}) [Đã Việt hóa nhãn cột]")

    print("[3/4] Cập nhật / Việt hóa toàn bộ 21 Biểu đồ & 4 Bảng điều khiển (Dashboards)...")
    existing_charts = {c.get("slice_name"): c for c in client.list_all("/api/v1/chart/")}
    existing_dashboards = {d.get("slug"): d for d in client.list_all("/api/v1/dashboard/")}

    d0_id, d0_charts = provision_legacy_overview_dashboard(
        client, datasets, existing_charts, existing_dashboards
    )
    print(f"      -> Dashboard 1 (id={d0_id}, {len(d0_charts)} biểu đồ): Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu")

    d1_id, d1_charts = provision_kmeans_dashboard(
        client, datasets, existing_charts, existing_dashboards
    )
    print(f"      -> Dashboard 2 (id={d1_id}, {len(d1_charts)} biểu đồ): Phân tích Phân cụm Dịch tễ — Đánh giá K & Hồ sơ Cụm")

    d2_id, d2_charts = provision_early_warning_dashboard(
        client, datasets, existing_charts, existing_dashboards
    )
    print(f"      -> Dashboard 3 (id={d2_id}, {len(d2_charts)} biểu đồ): Hệ thống Cảnh báo Sớm Nguy cơ Bùng phát Dịch")

    d3_id, d3_charts = provision_cdc_patients_dashboard(
        client, datasets, existing_charts, existing_dashboards
    )
    print(f"      -> Dashboard 4 (id={d3_id}, {len(d3_charts)} biểu đồ): Giám sát Thời gian thực — Bệnh nhân Ẩn danh & Tiêm chủng")

    public_base = "http://localhost:8089" if "superset:8088" in args.url else args.url.rstrip("/")
    print("[4/4] Hoàn tất! Mở trực tiếp 4 Bảng điều khiển tiếng Việt trên trình duyệt:")
    print(f"      1. Tổng quan 4 Nhóm K-Means        : {public_base}/superset/dashboard/covid19-kmeans-clusters/")
    print(f"      2. Đánh giá Số cụm K & Hồ sơ Cụm   : {public_base}/superset/dashboard/covid19-kmeans-mining/")
    print(f"      3. Cảnh báo Sớm Bùng phát (XGBoost): {public_base}/superset/dashboard/covid19-early-warning/")
    print(f"      4. Bệnh nhân Ẩn danh & Tiêm chủng  : {public_base}/superset/dashboard/covid19-cdc-patients-vaccination/")
    print("=" * 88)


if __name__ == "__main__":
    main()
