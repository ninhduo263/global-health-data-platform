-- ============================================================================
-- OLTP INITIALIZATION SCRIPT (MySQL 8.0) - NGUỒN 3: RELATIONAL DB
-- Giả lập Hệ thống Quản lý Phân bổ Vắc-xin & Khai báo Y tế Nội bộ (OLTP)
-- Phục vụ trích xuất Batch/CDC qua Debezium -> Kafka & Airflow -> MinIO/DWH
-- ============================================================================

CREATE DATABASE IF NOT EXISTS vaccination_oltp
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE vaccination_oltp;

-- Cấp quyền Replication cho user để Debezium có thể đọc MySQL Binlog
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'oltp_user'@'%';
FLUSH PRIVILEGES;

DROP TABLE IF EXISTS vaccination_records;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS vaccination_centers;

-- 1. Bảng danh mục cơ sở y tế / trung tâm tiêm chủng
CREATE TABLE IF NOT EXISTS vaccination_centers (
    center_id INT AUTO_INCREMENT PRIMARY KEY,
    center_code VARCHAR(20) NOT NULL UNIQUE,
    center_name VARCHAR(150) NOT NULL,
    province VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bảng danh sách bệnh nhân & hồ sơ khai báo y tế nội bộ (patients)
CREATE TABLE IF NOT EXISTS patients (
    patient_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    citizen_hash VARCHAR(64) NOT NULL UNIQUE,   -- Mã định danh công dân ẩn danh (SHA-256)
    full_name VARCHAR(120) NOT NULL,
    birth_year SMALLINT NOT NULL,
    gender ENUM('M', 'F', 'O') NOT NULL,
    province VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    health_status ENUM('HEALTHY', 'SUSPECTED', 'CONFIRMED_COVID', 'RECOVERED') NOT NULL DEFAULT 'HEALTHY',
    comorbidity VARCHAR(150) DEFAULT 'None',    -- Bệnh nền (Tiểu đường, Huyết áp, Tim mạch...)
    reported_date DATE NOT NULL,                -- Ngày khai báo y tế / ghi nhận ca bệnh
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 3. Bảng lịch sử tiêm chủng vận hành (vaccination_records)
CREATE TABLE IF NOT EXISTS vaccination_records (
    record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id BIGINT,
    citizen_hash VARCHAR(64) NOT NULL,
    center_id INT NOT NULL,
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    vaccine_type VARCHAR(50) NOT NULL,          -- Pfizer-BioNTech, AstraZeneca, Moderna, VeroCell
    dose_number TINYINT NOT NULL,               -- Mũi 1, 2, 3 (Booster)
    batch_number VARCHAR(30) NOT NULL,          -- Số lô vắc-xin phân bổ
    vaccination_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_center FOREIGN KEY (center_id) REFERENCES vaccination_centers(center_id),
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

-- ============================================================================
-- DỮ LIỆU VẬN HÀNH MẪU BAN ĐẦU (SEED DATA CHO NGUỒN 3)
-- ============================================================================
INSERT INTO vaccination_centers (center_code, center_name, province, country_code)
VALUES
    ('VN-HN-01', 'Viện Vệ sinh Dịch tễ Trung ương', 'Hà Nội', 'VNM'),
    ('VN-HCM-01', 'Viện Pasteur TP. Hồ Chí Minh', 'TP. Hồ Chí Minh', 'VNM'),
    ('VN-DN-01', 'Trung tâm Kiểm soát Bệnh tật Đà Nẵng', 'Đà Nẵng', 'VNM')
ON DUPLICATE KEY UPDATE center_name = VALUES(center_name);

INSERT INTO patients (citizen_hash, full_name, birth_year, gender, province, country_code, health_status, comorbidity, reported_date)
VALUES
    ('a1b2c3d4e5f60001', 'Nguyen Van An', 1985, 'M', 'Hà Nội', 'VNM', 'HEALTHY', 'None', '2021-08-15'),
    ('a1b2c3d4e5f60002', 'Tran Thi Binh', 1978, 'F', 'TP. Hồ Chí Minh', 'VNM', 'CONFIRMED_COVID', 'Hypertension', '2021-08-15'),
    ('a1b2c3d4e5f60003', 'Le Hoang Cuong', 1992, 'M', 'Hà Nội', 'VNM', 'SUSPECTED', 'None', '2021-08-16'),
    ('a1b2c3d4e5f60004', 'Pham Minh Duc', 1965, 'M', 'TP. Hồ Chí Minh', 'VNM', 'CONFIRMED_COVID', 'Diabetes', '2021-08-16'),
    ('a1b2c3d4e5f60005', 'Hoang Thu Ha', 1998, 'F', 'Đà Nẵng', 'VNM', 'RECOVERED', 'None', '2021-08-17'),
    ('a1b2c3d4e5f60006', 'Vu Quang Huy', 1989, 'M', 'Hà Nội', 'VNM', 'HEALTHY', 'None', '2021-08-17')
ON DUPLICATE KEY UPDATE health_status = VALUES(health_status);

INSERT INTO vaccination_records (patient_id, citizen_hash, center_id, country_code, vaccine_type, dose_number, batch_number, vaccination_date)
VALUES
    (1, 'a1b2c3d4e5f60001', 1, 'VNM', 'Pfizer-BioNTech', 1, 'LOT-PFZ-2021A', '2021-08-15'),
    (2, 'a1b2c3d4e5f60002', 2, 'VNM', 'AstraZeneca', 2, 'LOT-AZD-2021B', '2021-08-15'),
    (3, 'a1b2c3d4e5f60003', 1, 'VNM', 'Moderna', 1, 'LOT-MOD-2021A', '2021-08-16'),
    (4, 'a1b2c3d4e5f60004', 2, 'VNM', 'Pfizer-BioNTech', 2, 'LOT-PFZ-2021B', '2021-08-16'),
    (5, 'a1b2c3d4e5f60005', 3, 'VNM', 'AstraZeneca', 1, 'LOT-AZD-2021C', '2021-08-17'),
    (6, 'a1b2c3d4e5f60006', 1, 'VNM', 'VeroCell', 1, 'LOT-SIN-2021A', '2021-08-17');
