-- ============================================================================
-- OLTP INITIALIZATION SCRIPT (MySQL 8.0)
-- Giả lập CSDL Tiêm chủng nội bộ (Internal Vaccination OLTP Database)
-- Phục vụ Change Data Capture (CDC) qua Debezium -> Kafka
-- ============================================================================

CREATE DATABASE IF NOT EXISTS vaccination_oltp
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE vaccination_oltp;

-- Cấp quyền Replication cho user để Debezium có thể đọc MySQL Binlog
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'oltp_user'@'%';
FLUSH PRIVILEGES;

-- Bảng danh mục trung tâm tiêm chủng
CREATE TABLE IF NOT EXISTS vaccination_centers (
    center_id INT AUTO_INCREMENT PRIMARY KEY,
    center_code VARCHAR(20) NOT NULL UNIQUE,
    center_name VARCHAR(150) NOT NULL,
    province VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL DEFAULT 'VNM',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng giao dịch tiêm chủng (OLTP Transactional Table được Debezium lắng nghe)
CREATE TABLE IF NOT EXISTS vaccination_records (
    record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    citizen_hash VARCHAR(64) NOT NULL,          -- Mã định danh công dân đã ẩn danh (SHA-256)
    center_id INT NOT NULL,
    vaccine_type VARCHAR(50) NOT NULL,          -- Pfizer, AstraZeneca, Moderna, VeroCell...
    dose_number TINYINT NOT NULL,               -- Mũi 1, 2, 3 (Booster)
    vaccination_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_center FOREIGN KEY (center_id) REFERENCES vaccination_centers(center_id)
);

-- Chèn dữ liệu mẫu ban đầu
INSERT INTO vaccination_centers (center_code, center_name, province, country_code)
VALUES
    ('VN-HN-01', 'Viện Vệ sinh Dịch tễ Trung ương', 'Hà Nội', 'VNM'),
    ('VN-HCM-01', 'Viện Pasteur TP. Hồ Chí Minh', 'TP. Hồ Chí Minh', 'VNM');

INSERT INTO vaccination_records (citizen_hash, center_id, vaccine_type, dose_number, vaccination_date)
VALUES
    ('a1b2c3d4e5f60001', 1, 'Pfizer-BioNTech', 1, '2021-08-15'),
    ('a1b2c3d4e5f60002', 2, 'AstraZeneca', 2, '2021-08-15'),
    ('a1b2c3d4e5f60003', 1, 'Moderna', 1, '2021-08-16');

