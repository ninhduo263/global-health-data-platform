-- ============================================================================
-- DATA WAREHOUSE INITIALIZATION SCRIPT (PostgreSQL)
-- Dự án: Phân tích dữ liệu Y tế Công cộng (COVID-19 / Bệnh truyền nhiễm)
-- Mô hình: Snowflake Schema (schema: dwh)
-- ============================================================================

-- 1. Tạo database riêng cho Airflow (nếu dùng chung PostgreSQL instance) hoặc schema riêng cho DWH
CREATE SCHEMA IF NOT EXISTS dwh;
SET search_path TO dwh, public;

-- ============================================================================
-- BẢNG PHÂN CẤP ĐỊA LÝ (Sub-dimension chuẩn hóa cho Snowflake Schema)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_region (
    id SERIAL PRIMARY KEY,
    continent_code VARCHAR(10) NOT NULL,
    continent_name VARCHAR(100) NOT NULL,
    who_region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_continent_code UNIQUE (continent_code)
);

COMMENT ON TABLE dwh.dim_region IS 'Bảng chiều phân cấp khu vực/châu lục (chuẩn hóa từ dim_country trong mô hình Snowflake)';

-- ============================================================================
-- 1. BẢNG CHIỀU QUỐC GIA (dim_country)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_country (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,          -- Mã ISO-3 (VD: VNM, USA, JPN)
    country_name VARCHAR(150) NOT NULL,         -- Tên quốc gia
    population BIGINT CHECK (population >= 0),  -- Quy mô dân số
    gdp NUMERIC(18, 2) CHECK (gdp >= 0),        -- GDP ( hoặc GDP bình quân đầu người )
    beds_per_1000 NUMERIC(8, 3) CHECK (beds_per_1000 >= 0), -- Số giường bệnh / 1000 dân
    region_id INT REFERENCES dwh.dim_region(id) ON DELETE SET NULL, -- FK nối tới nhánh Snowflake dim_region
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_country_code UNIQUE (country_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_country_code ON dwh.dim_country(country_code);
COMMENT ON TABLE dwh.dim_country IS 'Bảng chiều thông tin nhân khẩu học và năng lực y tế của các quốc gia';

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
COMMENT ON TABLE dwh.dim_date IS 'Bảng chiều thời gian phục vụ tổng hợp dữ liệu theo ngày/tháng/năm/cuối tuần';

-- ============================================================================
-- 3. BẢNG CHIỀU CHỈ SỐ Y TẾ (dim_indicator)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_indicator (
    id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(50) NOT NULL UNIQUE, -- Mã chỉ số (VD: COVID_CONFIRMED, VAX_BOOSTER, ICU_ADM)
    description TEXT NOT NULL                   -- Mô tả chi tiết ý nghĩa và nguồn thu thập chỉ số
);

COMMENT ON TABLE dwh.dim_indicator IS 'Bảng chiều danh mục các chỉ số giám sát dịch tễ và y tế công cộng';

-- ============================================================================
-- 4. BẢNG SỰ KIỆN CA BỆNH & TIÊM CHỦNG (fact_cases)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_cases (
    id BIGSERIAL PRIMARY KEY,
    country_id INT NOT NULL REFERENCES dwh.dim_country(id) ON DELETE RESTRICT,
    date_id INT NOT NULL REFERENCES dwh.dim_date(date_id) ON DELETE RESTRICT,
    indicator_id INT REFERENCES dwh.dim_indicator(id) ON DELETE SET NULL,
    new_cases INT DEFAULT 0 CHECK (new_cases >= 0),                -- Số ca mắc mới trong ngày
    new_deaths INT DEFAULT 0 CHECK (new_deaths >= 0),              -- Số ca tử vong mới trong ngày
    total_vaccinations BIGINT DEFAULT 0 CHECK (total_vaccinations >= 0), -- Tổng số liều vắc-xin lũy kế
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Đảm bảo tính duy nhất cho mỗi bản ghi theo Quốc gia + Ngày (+ Chỉ số)
    CONSTRAINT uq_fact_country_date UNIQUE (country_id, date_id)
);

-- Tạo Index tối ưu hóa truy vấn phân tích (OLAP) và kết nối BI (Superset)
CREATE INDEX IF NOT EXISTS idx_fact_cases_country_id ON dwh.fact_cases(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_cases_date_id ON dwh.fact_cases(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_cases_indicator_id ON dwh.fact_cases(indicator_id);

COMMENT ON TABLE dwh.fact_cases IS 'Bảng Fact trung tâm lưu trữ số liệu ca nhiễm mới, tử vong và tiêm chủng theo ngày';

-- ============================================================================
-- DỮ LIỆU KHỞI TẠO MẪU (SEED DATA)
-- ============================================================================
INSERT INTO dwh.dim_region (continent_code, continent_name, who_region)
VALUES
    ('AS', 'Asia', 'WPRO'),
    ('EU', 'Europe', 'EURO'),
    ('NA', 'North America', 'AMRO')
ON CONFLICT (continent_code) DO NOTHING;

INSERT INTO dwh.dim_indicator (indicator_code, description)
VALUES
    ('COVID19_DAILY_SURVEILLANCE', 'Giám sát dịch tễ COVID-19 hàng ngày (Ca mắc mới, Tử vong mới, Lũy kế tiêm chủng)'),
    ('WHO_RESPIRATORY_INDEX', 'Chỉ số bệnh truyền nhiễm đường hô hấp theo chuẩn WHO')
ON CONFLICT (indicator_code) DO NOTHING;

INSERT INTO dwh.dim_country (country_code, country_name, population, gdp, beds_per_1000, region_id)
VALUES
    ('VNM', 'Vietnam', 98186856, 408800000000.00, 2.600, 1),
    ('USA', 'United States', 338289857, 25462700000000.00, 2.870, 3),
    ('DEU', 'Germany', 83369843, 4072190000000.00, 8.000, 2)
ON CONFLICT (country_code) DO NOTHING;

-- Khởi tạo sẵn một số ngày mẫu trong dim_date (năm 2020 - 2023) bằng generate_series
INSERT INTO dwh.dim_date (date_id, full_date, year, month, day, is_weekend)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum::DATE AS full_date,
    EXTRACT(YEAR FROM datum)::SMALLINT AS year,
    EXTRACT(MONTH FROM datum)::SMALLINT AS month,
    EXTRACT(DAY FROM datum)::SMALLINT AS day,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) AS datum
ON CONFLICT (date_id) DO NOTHING;

