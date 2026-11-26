CREATE TABLE IF NOT EXISTS dwh.dim_vaccination_center (
    id INT PRIMARY KEY,
    center_code VARCHAR(20),
    center_name VARCHAR(150) NOT NULL,
    province VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    is_placeholder BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dwh.fact_vaccination (
    record_id BIGINT PRIMARY KEY,
    center_id INT REFERENCES dwh.dim_vaccination_center(id) ON DELETE SET NULL,
    date_id INT NOT NULL REFERENCES dwh.dim_date(date_id) ON DELETE RESTRICT,
    vaccine_type VARCHAR(50) NOT NULL,
    dose_number SMALLINT NOT NULL CHECK (dose_number > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_vaccination_center_id
    ON dwh.fact_vaccination(center_id);
CREATE INDEX IF NOT EXISTS idx_fact_vaccination_date_id
    ON dwh.fact_vaccination(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_vaccination_vaccine_type
    ON dwh.fact_vaccination(vaccine_type);

ALTER TABLE dwh.fact_cases
    DROP CONSTRAINT IF EXISTS fact_cases_new_cases_check,
    DROP CONSTRAINT IF EXISTS fact_cases_new_deaths_check;

ALTER TABLE dwh.fact_cases
    ADD COLUMN IF NOT EXISTS new_tests BIGINT NOT NULL DEFAULT 0
        CHECK (new_tests >= 0),
    ADD COLUMN IF NOT EXISTS new_vaccinations BIGINT NOT NULL DEFAULT 0
        CHECK (new_vaccinations >= 0),
    ADD COLUMN IF NOT EXISTS people_vaccinated BIGINT NOT NULL DEFAULT 0
        CHECK (people_vaccinated >= 0),
    ADD COLUMN IF NOT EXISTS people_fully_vaccinated BIGINT NOT NULL DEFAULT 0
        CHECK (people_fully_vaccinated >= 0),
    ADD COLUMN IF NOT EXISTS total_cases BIGINT NOT NULL DEFAULT 0
        CHECK (total_cases >= 0),
    ADD COLUMN IF NOT EXISTS total_deaths BIGINT NOT NULL DEFAULT 0
        CHECK (total_deaths >= 0);

DROP VIEW IF EXISTS dwh.vw_dashboard_cases;
CREATE VIEW dwh.vw_dashboard_cases AS
SELECT
    d.full_date AS report_date,
    d.year,
    d.month,
    c.country_code,
    c.country_name,
    r.continent_name,
    r.continent_code AS who_region,
    f.new_cases,
    f.new_deaths,
    f.total_vaccinations,
    f.total_cases,
    f.total_deaths,
    f.new_vaccinations,
    f.people_vaccinated,
    f.people_fully_vaccinated
FROM dwh.fact_cases AS f
JOIN dwh.dim_country AS c ON c.id = f.country_id
JOIN dwh.dim_date AS d ON d.date_id = f.date_id
LEFT JOIN dwh.dim_region AS r ON r.id = c.region_id;

CREATE TABLE IF NOT EXISTS dwh.fact_who_covid_daily (
    report_date DATE NOT NULL,
    country_code CHAR(2) NOT NULL,
    country_name VARCHAR(150) NOT NULL,
    who_region VARCHAR(10) NOT NULL,
    new_cases BIGINT NOT NULL DEFAULT 0,
    cumulative_cases BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_cases >= 0),
    new_deaths BIGINT NOT NULL DEFAULT 0,
    cumulative_deaths BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_deaths >= 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, country_code)
);

CREATE INDEX IF NOT EXISTS idx_fact_who_covid_daily_region_date
    ON dwh.fact_who_covid_daily(who_region, report_date);

CREATE OR REPLACE VIEW dwh.vw_who_covid_daily AS
SELECT
    report_date,
    country_code,
    country_name,
    who_region,
    new_cases,
    cumulative_cases,
    new_deaths,
    cumulative_deaths
FROM dwh.fact_who_covid_daily;

CREATE OR REPLACE VIEW dwh.vw_dashboard_vaccinations AS
SELECT
    d.full_date AS vaccination_date,
    d.year,
    d.month,
    c.center_code,
    c.center_name,
    c.province,
    c.country_code,
    f.vaccine_type,
    f.dose_number,
    COUNT(*) AS vaccination_count
FROM dwh.fact_vaccination AS f
JOIN dwh.dim_date AS d ON d.date_id = f.date_id
LEFT JOIN dwh.dim_vaccination_center AS c ON c.id = f.center_id
GROUP BY
    d.full_date,
    d.year,
    d.month,
    c.center_code,
    c.center_name,
    c.province,
    c.country_code,
    f.vaccine_type,
    f.dose_number;

CREATE TABLE IF NOT EXISTS dwh.fact_weekly_covid_cluster (
    week_start DATE NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(150) NOT NULL,
    cluster_id SMALLINT NOT NULL CHECK (cluster_id >= 0),
    cluster_count SMALLINT NOT NULL CHECK (cluster_count > 0),
    weekly_cases BIGINT NOT NULL CHECK (weekly_cases >= 0),
    weekly_deaths BIGINT NOT NULL CHECK (weekly_deaths >= 0),
    population BIGINT NOT NULL CHECK (population > 0),
    cases_per_100k NUMERIC(14, 4) NOT NULL CHECK (cases_per_100k >= 0),
    deaths_per_100k NUMERIC(14, 4) NOT NULL CHECK (deaths_per_100k >= 0),
    case_growth_pct NUMERIC(14, 4),
    model_version VARCHAR(40) NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (week_start, country_code, model_version)
);

CREATE INDEX IF NOT EXISTS idx_weekly_covid_cluster_week_cluster
    ON dwh.fact_weekly_covid_cluster(week_start, cluster_id);

-- ============================================================================
-- CDC: HỒ SƠ BỆNH NHÂN (ẩn danh - không lưu full_name / citizen_hash)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_patient (
    patient_id BIGINT PRIMARY KEY,
    birth_year SMALLINT,
    gender CHAR(1),
    province VARCHAR(100),
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    health_status VARCHAR(20),
    comorbidity VARCHAR(150),
    reported_date_id INT REFERENCES dwh.dim_date(date_id) ON DELETE RESTRICT,
    is_placeholder BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE dwh.fact_vaccination
    ADD COLUMN IF NOT EXISTS patient_id BIGINT
        REFERENCES dwh.dim_patient(patient_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_fact_vaccination_patient_id
    ON dwh.fact_vaccination(patient_id);

CREATE OR REPLACE VIEW dwh.vw_dashboard_patients AS
SELECT
    d.full_date AS reported_date,
    d.year,
    d.month,
    p.province,
    p.country_code,
    p.gender,
    CASE
        WHEN p.birth_year IS NULL OR d.year IS NULL THEN 'Unknown'
        WHEN d.year - p.birth_year < 18 THEN '0-17'
        WHEN d.year - p.birth_year < 40 THEN '18-39'
        WHEN d.year - p.birth_year < 60 THEN '40-59'
        ELSE '60+'
    END AS age_group,
    p.health_status,
    p.comorbidity,
    COUNT(*) AS patient_count,
    CASE p.gender
        WHEN 'M' THEN 'Nam'
        WHEN 'F' THEN 'Nữ'
        ELSE 'Khác'
    END AS gender_vi,
    CASE
        WHEN p.birth_year IS NULL OR d.year IS NULL THEN 'Chưa rõ'
        WHEN d.year - p.birth_year < 18 THEN '1. Dưới 18 tuổi'
        WHEN d.year - p.birth_year < 40 THEN '2. Từ 18 – 39 tuổi'
        WHEN d.year - p.birth_year < 60 THEN '3. Từ 40 – 59 tuổi'
        ELSE '4. Từ 60 tuổi trở lên'
    END AS age_group_vi,
    CASE p.health_status
        WHEN 'CONFIRMED_COVID' THEN 'Dương tính COVID-19'
        WHEN 'SUSPECTED' THEN 'Nghi nhiễm'
        WHEN 'RECOVERED' THEN 'Đã khỏi bệnh'
        WHEN 'HEALTHY' THEN 'Bình thường'
        ELSE COALESCE(p.health_status, 'Chưa rõ')
    END AS health_status_vi,
    CASE p.comorbidity
        WHEN 'None' THEN 'Không có bệnh nền'
        WHEN 'Hypertension' THEN 'Tăng huyết áp'
        WHEN 'Diabetes' THEN 'Tiểu đường'
        WHEN 'Cardiovascular' THEN 'Bệnh tim mạch'
        WHEN 'Chronic Respiratory' THEN 'Bệnh hô hấp mạn tính'
        WHEN 'Obesity' THEN 'Béo phì'
        ELSE COALESCE(p.comorbidity, 'Khác')
    END AS comorbidity_vi
FROM dwh.dim_patient AS p
LEFT JOIN dwh.dim_date AS d ON d.date_id = p.reported_date_id
WHERE NOT p.is_placeholder
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14;

CREATE OR REPLACE VIEW dwh.vw_vaccination_by_patient AS
SELECT
    d.full_date AS vaccination_date,
    c.province AS center_province,
    f.vaccine_type,
    f.dose_number,
    p.gender,
    CASE
        WHEN p.birth_year IS NULL THEN 'Unknown'
        WHEN d.year - p.birth_year < 18 THEN '0-17'
        WHEN d.year - p.birth_year < 40 THEN '18-39'
        WHEN d.year - p.birth_year < 60 THEN '40-59'
        ELSE '60+'
    END AS age_group,
    p.health_status,
    p.comorbidity,
    COUNT(*) AS vaccination_count,
    'Mũi ' || f.dose_number::text AS dose_label_vi,
    CASE p.gender
        WHEN 'M' THEN 'Nam'
        WHEN 'F' THEN 'Nữ'
        ELSE 'Khác'
    END AS gender_vi,
    CASE
        WHEN p.birth_year IS NULL THEN 'Chưa rõ'
        WHEN d.year - p.birth_year < 18 THEN '1. Dưới 18 tuổi'
        WHEN d.year - p.birth_year < 40 THEN '2. Từ 18 – 39 tuổi'
        WHEN d.year - p.birth_year < 60 THEN '3. Từ 40 – 59 tuổi'
        ELSE '4. Từ 60 tuổi trở lên'
    END AS age_group_vi,
    CASE p.health_status
        WHEN 'CONFIRMED_COVID' THEN 'Dương tính COVID-19'
        WHEN 'SUSPECTED' THEN 'Nghi nhiễm'
        WHEN 'RECOVERED' THEN 'Đã khỏi bệnh'
        WHEN 'HEALTHY' THEN 'Bình thường'
        ELSE COALESCE(p.health_status, 'Chưa rõ')
    END AS health_status_vi,
    CASE p.comorbidity
        WHEN 'None' THEN 'Không có bệnh nền'
        WHEN 'Hypertension' THEN 'Tăng huyết áp'
        WHEN 'Diabetes' THEN 'Tiểu đường'
        WHEN 'Cardiovascular' THEN 'Bệnh tim mạch'
        WHEN 'Chronic Respiratory' THEN 'Bệnh hô hấp mạn tính'
        WHEN 'Obesity' THEN 'Béo phì'
        ELSE COALESCE(p.comorbidity, 'Khác')
    END AS comorbidity_vi
FROM dwh.fact_vaccination AS f
JOIN dwh.dim_date AS d ON d.date_id = f.date_id
LEFT JOIN dwh.dim_vaccination_center AS c ON c.id = f.center_id
LEFT JOIN dwh.dim_patient AS p ON p.patient_id = f.patient_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14;

-- ============================================================================
-- K-MEANS: ĐÁNH GIÁ SỐ CỤM, HỒ SƠ CỤM VÀ TỌA ĐỘ PCA
-- ============================================================================
ALTER TABLE dwh.fact_weekly_covid_cluster
    ADD COLUMN IF NOT EXISTS cluster_label VARCHAR(60),
    ADD COLUMN IF NOT EXISTS pca_1 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS pca_2 DOUBLE PRECISION;

CREATE OR REPLACE VIEW dwh.vw_weekly_covid_clusters AS
SELECT
    week_start,
    country_code,
    country_name,
    cluster_id,
    cluster_count,
    weekly_cases,
    weekly_deaths,
    population,
    cases_per_100k,
    deaths_per_100k,
    case_growth_pct,
    model_version,
    computed_at,
    cluster_label,
    pca_1,
    pca_2
FROM dwh.fact_weekly_covid_cluster;

CREATE TABLE IF NOT EXISTS dwh.fact_kmeans_evaluation (
    model_version VARCHAR(40) NOT NULL,
    k SMALLINT NOT NULL CHECK (k >= 2),
    inertia DOUBLE PRECISION NOT NULL,
    silhouette DOUBLE PRECISION,
    davies_bouldin DOUBLE PRECISION,
    calinski_harabasz DOUBLE PRECISION,
    total_rows INT NOT NULL,
    silhouette_sample_rows INT NOT NULL,
    is_selected BOOLEAN NOT NULL DEFAULT FALSE,
    is_best_silhouette BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_version, k)
);

CREATE TABLE IF NOT EXISTS dwh.fact_kmeans_cluster_profile (
    model_version VARCHAR(40) NOT NULL,
    cluster_id SMALLINT NOT NULL CHECK (cluster_id >= 0),
    cluster_label VARCHAR(60) NOT NULL,
    member_count INT NOT NULL,
    member_share DOUBLE PRECISION NOT NULL,
    avg_cases_per_100k DOUBLE PRECISION NOT NULL,
    median_cases_per_100k DOUBLE PRECISION NOT NULL,
    avg_deaths_per_100k DOUBLE PRECISION NOT NULL,
    median_case_growth_pct DOUBLE PRECISION,
    centroid_log_cases DOUBLE PRECISION NOT NULL,
    centroid_log_deaths DOUBLE PRECISION NOT NULL,
    centroid_log_change DOUBLE PRECISION NOT NULL,
    avg_silhouette DOUBLE PRECISION,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_version, cluster_id)
);

CREATE OR REPLACE VIEW dwh.vw_kmeans_evaluation AS
SELECT * FROM dwh.fact_kmeans_evaluation;

CREATE OR REPLACE VIEW dwh.vw_kmeans_cluster_profile AS
SELECT * FROM dwh.fact_kmeans_cluster_profile;

-- ============================================================================
-- CẢNH BÁO SỚM (XGBoost): DỰ BÁO NGUY CƠ BÙNG PHÁT CA MẮC
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_early_warning (
    week_start DATE NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(150) NOT NULL,
    model_version VARCHAR(40) NOT NULL,
    model_name VARCHAR(40) NOT NULL,
    horizon_weeks SMALLINT NOT NULL CHECK (horizon_weeks > 0),
    prediction_split VARCHAR(10) NOT NULL CHECK (prediction_split IN ('test', 'forecast')),
    cases_per_100k DOUBLE PRECISION NOT NULL,
    case_growth_pct DOUBLE PRECISION,
    cluster_id SMALLINT,
    surge_probability DOUBLE PRECISION NOT NULL
        CHECK (surge_probability BETWEEN 0 AND 1),
    decision_threshold DOUBLE PRECISION NOT NULL,
    predicted_surge BOOLEAN NOT NULL,
    alert_level VARCHAR(10) NOT NULL CHECK (alert_level IN ('HIGH', 'WATCH', 'LOW')),
    actual_surge BOOLEAN,
    future_cases_per_100k DOUBLE PRECISION,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (week_start, country_code, model_version)
);

CREATE INDEX IF NOT EXISTS idx_early_warning_split_week
    ON dwh.fact_early_warning(prediction_split, week_start);

CREATE TABLE IF NOT EXISTS dwh.fact_early_warning_evaluation (
    model_version VARCHAR(40) NOT NULL,
    model_name VARCHAR(40) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    decision_threshold DOUBLE PRECISION,
    accuracy DOUBLE PRECISION,
    precision_score DOUBLE PRECISION,
    recall_score DOUBLE PRECISION,
    f1_score DOUBLE PRECISION,
    roc_auc DOUBLE PRECISION,
    pr_auc DOUBLE PRECISION,
    true_positive INT,
    false_positive INT,
    true_negative INT,
    false_negative INT,
    train_rows INT NOT NULL,
    test_rows INT NOT NULL,
    test_positive_rate DOUBLE PRECISION,
    test_start_week DATE,
    test_end_week DATE,
    horizon_weeks SMALLINT NOT NULL,
    surge_ratio DOUBLE PRECISION NOT NULL,
    min_cases_per_100k DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_version, model_name)
);

CREATE TABLE IF NOT EXISTS dwh.fact_early_warning_feature_importance (
    model_version VARCHAR(40) NOT NULL,
    model_name VARCHAR(40) NOT NULL,
    feature_name VARCHAR(60) NOT NULL,
    importance DOUBLE PRECISION NOT NULL,
    importance_rank SMALLINT NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_version, feature_name)
);

CREATE OR REPLACE VIEW dwh.vw_early_warning AS
SELECT
    ew.*,
    CASE ew.alert_level
        WHEN 'HIGH' THEN 'Nguy cơ cao'
        WHEN 'WATCH' THEN 'Cần theo dõi'
        WHEN 'LOW' THEN 'Nguy cơ thấp'
        ELSE ew.alert_level
    END AS alert_level_vi,
    CASE ew.prediction_split
        WHEN 'test' THEN 'Kiểm thử lịch sử'
        WHEN 'forecast' THEN 'Dự báo tương lai'
        ELSE ew.prediction_split
    END AS prediction_split_vi
FROM dwh.fact_early_warning AS ew;

-- Cảnh báo mới nhất của từng quốc gia (tuần gần nhất chưa có nhãn thực tế)
CREATE OR REPLACE VIEW dwh.vw_early_warning_latest AS
SELECT DISTINCT ON (ew.country_code, ew.model_version)
    ew.*,
    CASE ew.alert_level
        WHEN 'HIGH' THEN 'Nguy cơ cao'
        WHEN 'WATCH' THEN 'Cần theo dõi'
        WHEN 'LOW' THEN 'Nguy cơ thấp'
        ELSE ew.alert_level
    END AS alert_level_vi,
    CASE ew.prediction_split
        WHEN 'test' THEN 'Kiểm thử lịch sử'
        WHEN 'forecast' THEN 'Dự báo tương lai'
        ELSE ew.prediction_split
    END AS prediction_split_vi
FROM dwh.fact_early_warning AS ew
WHERE ew.prediction_split = 'forecast'
ORDER BY ew.country_code, ew.model_version, ew.week_start DESC;

CREATE OR REPLACE VIEW dwh.vw_early_warning_evaluation AS
SELECT
    ev.*,
    CASE ev.model_name
        WHEN 'xgboost' THEN 'XGBoost (Cây tăng cường)'
        WHEN 'hist_gradient_boosting' THEN 'Gradient Boosting'
        WHEN 'logistic_regression' THEN 'Hồi quy Logistic'
        WHEN 'rule_growth_25pct' THEN 'Luật cơ sở (Tăng ≥ 25%)'
        ELSE ev.model_name
    END AS model_name_vi
FROM dwh.fact_early_warning_evaluation AS ev;

CREATE OR REPLACE VIEW dwh.vw_early_warning_feature_importance AS
SELECT
    fi.*,
    CASE fi.feature_name
        WHEN 'log_cases_per_100k' THEN 'Mức ca mắc hiện tại (log/100k)'
        WHEN 'log_cases_lag_1' THEN 'Mức ca mắc trễ 1 tuần'
        WHEN 'log_cases_lag_2' THEN 'Mức ca mắc trễ 2 tuần'
        WHEN 'log_cases_lag_3' THEN 'Mức ca mắc trễ 3 tuần'
        WHEN 'cases_roll4_mean' THEN 'Trung bình ca mắc 4 tuần'
        WHEN 'cases_roll4_std' THEN 'Độ biến động ca mắc 4 tuần'
        WHEN 'growth_1w' THEN 'Tốc độ tăng ca 1 tuần'
        WHEN 'growth_2w' THEN 'Tốc độ tăng ca 2 tuần'
        WHEN 'growth_accel' THEN 'Gia tốc lây nhiễm'
        WHEN 'log_deaths_per_100k' THEN 'Mức tử vong hiện tại (log/100k)'
        WHEN 'death_growth_1w' THEN 'Tốc độ tăng tử vong 1 tuần'
        WHEN 'cfr_roll4' THEN 'Tỷ lệ tử vong/ca mắc (4 tuần)'
        WHEN 'vax_any_pct' THEN 'Tỷ lệ tiêm ít nhất 1 mũi'
        WHEN 'vax_full_pct' THEN 'Tỷ lệ tiêm chủng đầy đủ'
        WHEN 'week_sin' THEN 'Chu kỳ mùa vụ (Sin tuần)'
        WHEN 'week_cos' THEN 'Chu kỳ mùa vụ (Cos tuần)'
        WHEN 'global_mean_growth_1w' THEN 'Xu hướng tăng toàn cầu cùng tuần'
        WHEN 'cluster_id' THEN 'Nhóm mức dịch K-Means'
        ELSE fi.feature_name
    END AS feature_name_vi
FROM dwh.fact_early_warning_feature_importance AS fi;
