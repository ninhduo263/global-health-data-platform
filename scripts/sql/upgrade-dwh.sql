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
    computed_at
FROM dwh.fact_weekly_covid_cluster;
