-- ============================================================================
-- DATA WAREHOUSE INITIALIZATION SCRIPT (PostgreSQL)
-- Dự án: Phân tích dữ liệu Y tế Công cộng (COVID-19 / Bệnh truyền nhiễm)
-- Mô hình: Snowflake Schema tích hợp 3 nguồn dữ liệu (OWID API + WHO CSV + MySQL OLTP)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS dwh;
SET search_path TO dwh, public;

-- ============================================================================
-- BẢNG PHÂN CẤP ĐỊA LÝ (Sub-dimension chuẩn hóa cho Snowflake Schema - Nguồn 2)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_region (
    id SERIAL PRIMARY KEY,
    continent_code VARCHAR(10) NOT NULL,
    continent_name VARCHAR(100) NOT NULL,
    who_region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_continent_code UNIQUE (continent_code)
);

COMMENT ON TABLE dwh.dim_region IS 'Bảng chiều phân cấp khu vực/châu lục theo chuẩn WHO (Snowflake Sub-dimension)';

-- ============================================================================
-- 1. BẢNG CHIỀU QUỐC GIA (dim_country) - Nạp từ Nguồn 2 (WHO Global Health Observatory CSV)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_country (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,                     -- Mã ISO-3 (VD: VNM, USA, DEU)
    country_name VARCHAR(150) NOT NULL,                    -- Tên quốc gia
    population BIGINT CHECK (population >= 0),             -- Quy mô dân số (Nguồn 2: WHO)
    gdp NUMERIC(18, 2) CHECK (gdp >= 0),                   -- Tổng GDP (Nguồn 2: WHO)
    beds_per_1000 NUMERIC(8, 3) CHECK (beds_per_1000 >= 0),-- Số giường bệnh / 1000 dân (Nguồn 2: WHO)
    life_expectancy NUMERIC(5, 2) CHECK (life_expectancy >= 0), -- Tuổi thọ trung bình (Nguồn 2: WHO)
    region_id INT REFERENCES dwh.dim_region(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_country_code UNIQUE (country_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_country_code ON dwh.dim_country(country_code);
COMMENT ON TABLE dwh.dim_country IS 'Bảng chiều hạ tầng y tế & nhân khẩu học từ WHO Global Health Observatory (Nguồn 2)';

-- ============================================================================
-- 2. BẢNG CHIỀU THỜI GIAN (dim_date)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id INT PRIMARY KEY,                    -- Định dạng YYYYMMDD (VD: 20210815)
    full_date DATE NOT NULL UNIQUE,             -- Ngày đầy đủ (YYYY-MM-DD)
    year SMALLINT NOT NULL,                     -- Năm
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12), -- Tháng (1-12)
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),     -- Ngày trong tháng (1-31)
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE   -- Cờ đánh dấu cuối tuần (Thứ 7, Chủ Nhật)
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dwh.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dwh.dim_date(year, month);
COMMENT ON TABLE dwh.dim_date IS 'Bảng chiều thời gian phục vụ tổng hợp chuỗi thời gian';

-- ============================================================================
-- 3. BẢNG CHIỀU CHỈ SỐ Y TẾ (dim_indicator)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_indicator (
    id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(50) NOT NULL UNIQUE, -- Mã chỉ số (VD: MULTI_SOURCE_COVID19)
    description TEXT NOT NULL                   -- Mô tả chi tiết ý nghĩa và nguồn thu thập chỉ số
);

COMMENT ON TABLE dwh.dim_indicator IS 'Bảng chiều danh mục các chỉ số giám sát dịch tễ đa nguồn';

-- ============================================================================
-- 4. BẢNG SỰ KIỆN CA BỆNH & TIÊM CHỦNG (fact_cases)
-- Kết hợp Nguồn 1 (OWID API: ca nhiễm, tử vong, xét nghiệm)
--       + Nguồn 3 (MySQL OLTP: khai báo y tế & mũi tiêm nội bộ)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_cases (
    id BIGSERIAL PRIMARY KEY,
    country_id INT NOT NULL REFERENCES dwh.dim_country(id) ON DELETE RESTRICT,
    date_id INT NOT NULL REFERENCES dwh.dim_date(date_id) ON DELETE RESTRICT,
    indicator_id INT REFERENCES dwh.dim_indicator(id) ON DELETE SET NULL,
    new_cases INT DEFAULT 0 CHECK (new_cases >= 0),                      -- Nguồn 1 (OWID API)
    new_deaths INT DEFAULT 0 CHECK (new_deaths >= 0),                    -- Nguồn 1 (OWID API)
    new_tests INT DEFAULT 0 CHECK (new_tests >= 0),                      -- Nguồn 1 (OWID API)
    total_vaccinations BIGINT DEFAULT 0 CHECK (total_vaccinations >= 0), -- Nguồn 1 + Nguồn 3
    internal_vax_doses INT DEFAULT 0 CHECK (internal_vax_doses >= 0),    -- Nguồn 3 (MySQL OLTP)
    internal_patients_declared INT DEFAULT 0 CHECK (internal_patients_declared >= 0), -- Nguồn 3 (MySQL OLTP)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_country_date UNIQUE (country_id, date_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_cases_country_id ON dwh.fact_cases(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_cases_date_id ON dwh.fact_cases(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_cases_indicator_id ON dwh.fact_cases(indicator_id);

COMMENT ON TABLE dwh.fact_cases IS 'Bảng Fact trung tâm hợp nhất dữ liệu chuỗi thời gian OWID API (Nguồn 1) và CSDL vận hành MySQL OLTP (Nguồn 3)';

-- ============================================================================
-- DỮ LIỆU KHỞI TẠO MẪU CHO BẢNG LỊCH (dim_date)
-- ============================================================================
INSERT INTO dwh.dim_date (date_id, full_date, year, month, day, is_weekend)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum::DATE AS full_date,
    EXTRACT(YEAR FROM datum)::SMALLINT AS year,
    EXTRACT(MONTH FROM datum)::SMALLINT AS month,
    EXTRACT(DAY FROM datum)::SMALLINT AS day,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS datum
ON CONFLICT (date_id) DO NOTHING;

-- ============================================================================
-- 5. VIEW PHÂN TÍCH TỔNG HỢP CHO APACHE SUPERSET (Data Mart)
-- ============================================================================
CREATE OR REPLACE VIEW dwh.vw_covid19_analytics AS
SELECT
    f.id AS fact_id,
    c.country_code,
    c.country_name,
    r.continent_name,
    r.who_region,
    c.population,
    c.gdp,
    c.beds_per_1000,
    c.life_expectancy,
    d.date_id,
    d.full_date,
    d.year,
    d.month,
    d.day,
    d.is_weekend,
    f.new_cases,
    f.new_deaths,
    f.new_tests,
    f.total_vaccinations,
    f.internal_vax_doses,
    f.internal_patients_declared,
    -- 1. Tỷ lệ tử vong thô CFR (Case Fatality Rate %)
    ROUND(CASE WHEN f.new_cases > 0 THEN (f.new_deaths::numeric / f.new_cases::numeric) * 100.0 ELSE 0.0 END, 2) AS case_fatality_rate_pct,
    -- 2. Tỷ lệ ca nhiễm trên 100.000 dân
    ROUND(CASE WHEN c.population > 0 THEN (f.new_cases::numeric / c.population::numeric) * 100000.0 ELSE 0.0 END, 2) AS cases_per_100k_pop,
    -- 3. Tỷ lệ tử vong trên 100.000 dân
    ROUND(CASE WHEN c.population > 0 THEN (f.new_deaths::numeric / c.population::numeric) * 100000.0 ELSE 0.0 END, 2) AS deaths_per_100k_pop,
    -- 4. Tỷ lệ bao phủ vắc-xin (% dân số)
    ROUND(CASE WHEN c.population > 0 THEN (f.total_vaccinations::numeric / c.population::numeric) * 100.0 ELSE 0.0 END, 2) AS vaccination_coverage_pct,
    -- 5. Trung bình trượt 7 ngày ca nhiễm mới (7-Day Moving Average)
    ROUND(AVG(f.new_cases) OVER (
        PARTITION BY f.country_id
        ORDER BY d.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::numeric, 1) AS new_cases_7d_avg,
    -- 6. Trung bình trượt 7 ngày ca tử vong mới (7-Day Moving Average)
    ROUND(AVG(f.new_deaths) OVER (
        PARTITION BY f.country_id
        ORDER BY d.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::numeric, 1) AS new_deaths_7d_avg,
    -- 7. Tỷ lệ xét nghiệm dương tính (Positivity Rate %)
    ROUND(CASE WHEN f.new_tests > 0 THEN (f.new_cases::numeric / f.new_tests::numeric) * 100.0 ELSE 0.0 END, 2) AS test_positivity_rate_pct
FROM dwh.fact_cases f
JOIN dwh.dim_country c ON f.country_id = c.id
JOIN dwh.dim_date d ON f.date_id = d.date_id
LEFT JOIN dwh.dim_region r ON c.region_id = r.id;
