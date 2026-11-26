# -*- coding: utf-8 -*-
"""
Sinh dữ liệu giả lập quy mô lớn (mặc định 2.500 bệnh nhân & ~5.500 mũi tiêm)
vào MySQL OLTP (`vaccination_oltp`) để đồng bộ qua Debezium CDC sang PostgreSQL DWH,
phục vụ trực quan hóa trên Apache Superset (`vw_dashboard_patients`, `vw_vaccination_by_patient`).

Hỗ trợ chạy trực tiếp từ Windows Host (qua `docker exec mysql-oltp` bằng stdlib Python,
không phụ thuộc thư viện native bị chặn bởi Application Control) hoặc qua `pymysql`.
"""

import argparse
import hashlib
import os
import random
import subprocess
import sys
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

CENTERS = [
    ("VN-HN-01", "Viện Vệ sinh Dịch tễ Trung ương", "Hà Nội"),
    ("VN-HCM-01", "Viện Pasteur TP. Hồ Chí Minh", "TP. Hồ Chí Minh"),
    ("VN-DN-01", "Trung tâm Kiểm soát Bệnh tật Đà Nẵng", "Đà Nẵng"),
    ("VN-HP-01", "Trung tâm Kiểm soát Bệnh tật Hải Phòng", "Hải Phòng"),
    ("VN-CT-01", "Trung tâm Kiểm soát Bệnh tật Cần Thơ", "Cần Thơ"),
    ("VN-BD-01", "Bệnh viện Đa khoa Tỉnh Bình Dương", "Bình Dương"),
    ("VN-DNA-01", "Trung tâm Kiểm soát Bệnh tật Đồng Nai", "Đồng Nai"),
    ("VN-BG-01", "Trung tâm Y tế Dự phòng Bắc Giang", "Bắc Giang"),
    ("VN-KH-01", "Viện Pasteur Nha Trang", "Khánh Hòa"),
    ("VN-NA-01", "Trung tâm Kiểm soát Bệnh tật Nghệ An", "Nghệ An"),
]

PROVINCE_WEIGHTS = [
    ("TP. Hồ Chí Minh", 0.26),
    ("Hà Nội", 0.22),
    ("Bình Dương", 0.12),
    ("Đồng Nai", 0.09),
    ("Đà Nẵng", 0.07),
    ("Bắc Giang", 0.06),
    ("Hải Phòng", 0.05),
    ("Cần Thơ", 0.05),
    ("Khánh Hòa", 0.04),
    ("Nghệ An", 0.04),
]

LAST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu", "Vo", "Dang", "Bui", "Do"]
MIDDLE_NAMES_M = ["Van", "Minh", "Quang", "Hoang", "Duc", "Quoc", "Thanh", "Tuan"]
MIDDLE_NAMES_F = ["Thi", "Thu", "Ngoc", "Phuong", "Khanh", "Mai", "Thanh", " Bao"]
FIRST_NAMES_M = ["An", "Binh", "Cuong", "Dung", "Huy", "Khang", "Long", "Nam", "Phuc", "Son", "Thinh", "Trung"]
FIRST_NAMES_F = ["Anh", "Chi", "Giang", "Ha", "Hoa", "Lan", "Linh", "My", "Nga", "Nhung", "Thao", "Trang"]

VACCINE_WEIGHTS = [
    ("Pfizer-BioNTech", 0.38),
    ("AstraZeneca", 0.30),
    ("VeroCell", 0.18),
    ("Moderna", 0.14),
]


def sql_str(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def weighted_choice(rng: random.Random, weighted_items):
    items = [item for item, _ in weighted_items]
    weights = [weight for _, weight in weighted_items]
    return rng.choices(items, weights=weights, k=1)[0]


def sample_birth_year(rng: random.Random) -> int:
    bucket = rng.random()
    if bucket < 0.12:
        return rng.randint(2005, 2016)  # 0-17
    if bucket < 0.50:
        return rng.randint(1983, 2004)  # 18-39
    if bucket < 0.80:
        return rng.randint(1963, 1982)  # 40-59
    return rng.randint(1940, 1962)      # 60+


def sample_comorbidity(rng: random.Random, age: int) -> str:
    if age < 18:
        options = [("None", 0.94), ("Chronic Respiratory", 0.04), ("Obesity", 0.02)]
    elif age < 40:
        options = [
            ("None", 0.84),
            ("Obesity", 0.06),
            ("Hypertension", 0.05),
            ("Diabetes", 0.03),
            ("Chronic Respiratory", 0.02),
        ]
    elif age < 60:
        options = [
            ("None", 0.58),
            ("Hypertension", 0.18),
            ("Diabetes", 0.11),
            ("Cardiovascular", 0.06),
            ("Obesity", 0.04),
            ("Chronic Respiratory", 0.03),
        ]
    else:
        options = [
            ("None", 0.32),
            ("Hypertension", 0.28),
            ("Diabetes", 0.18),
            ("Cardiovascular", 0.14),
            ("Chronic Respiratory", 0.05),
            ("Obesity", 0.03),
        ]
    return weighted_choice(rng, options)


def sample_wave_date(rng: random.Random) -> date:
    wave = rng.random()
    if wave < 0.38:
        # Sóng Delta 2021 (tháng 6 - tháng 10/2021)
        start = date(2021, 6, 1)
        return start + timedelta(days=rng.randint(0, 150))
    if wave < 0.82:
        # Sóng Omicron đầu 2022 (tháng 11/2021 - tháng 5/2022)
        start = date(2021, 11, 1)
        return start + timedelta(days=rng.randint(0, 200))
    # Giai đoạn ổn định 2022 - 2023
    start = date(2022, 5, 21)
    return start + timedelta(days=rng.randint(0, 340))


def sample_health_status(rng: random.Random, comorbidity: str, province: str) -> str:
    high_burden = province in ("TP. Hồ Chí Minh", "Bình Dương", "Đồng Nai", "Hà Nội")
    has_comorb = comorbidity != "None"
    if high_burden and has_comorb:
        options = [
            ("HEALTHY", 0.38),
            ("CONFIRMED_COVID", 0.28),
            ("RECOVERED", 0.24),
            ("SUSPECTED", 0.10),
        ]
    elif high_burden or has_comorb:
        options = [
            ("HEALTHY", 0.50),
            ("RECOVERED", 0.22),
            ("CONFIRMED_COVID", 0.18),
            ("SUSPECTED", 0.10),
        ]
    else:
        options = [
            ("HEALTHY", 0.64),
            ("RECOVERED", 0.18),
            ("CONFIRMED_COVID", 0.10),
            ("SUSPECTED", 0.08),
        ]
    return weighted_choice(rng, options)


def generate_sql_batches(patient_count: int, seed: int, batch_size: int = 250):
    rng = random.Random(seed)
    statements = ["SET NAMES utf8mb4;"]

    center_values = ",\n".join(
        f"({sql_str(code)}, {sql_str(name)}, {sql_str(prov)}, 'VNM')"
        for code, name, prov in CENTERS
    )
    statements.append(
        f"""
        INSERT INTO vaccination_centers (center_code, center_name, province, country_code)
        VALUES
        {center_values}
        ON DUPLICATE KEY UPDATE
            center_name = VALUES(center_name),
            province = VALUES(province);
        """
    )

    province_to_code = {prov: code for code, _, prov in CENTERS}
    patients = []
    vaccinations = []

    for idx in range(1, patient_count + 1):
        citizen_hash = hashlib.sha256(f"btl-covid19-seed-patient-{idx:05d}".encode("utf-8")).hexdigest()
        gender = weighted_choice(rng, [("M", 0.49), ("F", 0.49), ("O", 0.02)])
        if gender == "F":
            full_name = f"{rng.choice(LAST_NAMES)} {rng.choice(MIDDLE_NAMES_F).strip()} {rng.choice(FIRST_NAMES_F)}"
        else:
            full_name = f"{rng.choice(LAST_NAMES)} {rng.choice(MIDDLE_NAMES_M)} {rng.choice(FIRST_NAMES_M)}"

        province = weighted_choice(rng, PROVINCE_WEIGHTS)
        reported_date = sample_wave_date(rng)
        birth_year = sample_birth_year(rng)
        age = max(0, reported_date.year - birth_year)
        comorbidity = sample_comorbidity(rng, age)
        health_status = sample_health_status(rng, comorbidity, province)

        patients.append(
            (
                citizen_hash,
                full_name,
                birth_year,
                gender,
                province,
                health_status,
                comorbidity,
                reported_date.isoformat(),
            )
        )

        # Số mũi tiêm (0 - 3 mũi, người lớn tuổi / bệnh nền ưu tiên tiêm đủ hơn)
        if age < 12:
            max_doses = weighted_choice(rng, [(0, 0.55), (1, 0.25), (2, 0.20)])
        elif age >= 50 or comorbidity != "None":
            max_doses = weighted_choice(rng, [(1, 0.08), (2, 0.37), (3, 0.55)])
        else:
            max_doses = weighted_choice(rng, [(0, 0.05), (1, 0.15), (2, 0.45), (3, 0.35)])

        primary_vaccine = weighted_choice(rng, VACCINE_WEIGHTS)
        first_vax_date = date(2021, 5, 1) + timedelta(days=rng.randint(0, 210))
        center_code = province_to_code.get(province, "VN-HN-01")

        for dose in range(1, max_doses + 1):
            if dose == 1:
                vax_date = first_vax_date
                vaccine = primary_vaccine
            elif dose == 2:
                vax_date = first_vax_date + timedelta(days=rng.randint(28, 56))
                vaccine = primary_vaccine
            else:
                vax_date = first_vax_date + timedelta(days=rng.randint(150, 240))
                vaccine = weighted_choice(rng, [("Pfizer-BioNTech", 0.65), ("Moderna", 0.35)])

            batch_no = f"LOT-{vaccine[:3].upper()}-{vax_date.year}{(vax_date.month - 1) // 3 + 1}"
            vaccinations.append((citizen_hash, center_code, vaccine, dose, batch_no, vax_date.isoformat()))

    for start in range(0, len(patients), batch_size):
        chunk = patients[start : start + batch_size]
        rows_sql = ",\n".join(
            f"({sql_str(ch)}, {sql_str(fn)}, {by}, {sql_str(gd)}, {sql_str(pv)}, 'VNM', "
            f"{sql_str(hs)}, {sql_str(cm)}, {sql_str(rd)})"
            for ch, fn, by, gd, pv, hs, cm, rd in chunk
        )
        statements.append(
            f"""
            INSERT INTO patients (
                citizen_hash, full_name, birth_year, gender, province,
                country_code, health_status, comorbidity, reported_date
            ) VALUES
            {rows_sql}
            ON DUPLICATE KEY UPDATE
                health_status = VALUES(health_status),
                comorbidity = VALUES(comorbidity),
                reported_date = VALUES(reported_date);
            """
        )

    # Xóa các mũi tiêm của tập seed cũ trước khi nạp lại để tránh trùng khi chạy nhiều lần
    statements.append(
        "DELETE FROM vaccination_records WHERE citizen_hash LIKE 'a%' AND LENGTH(citizen_hash) = 64 "
        "AND batch_number LIKE 'LOT-%';"
    )
    # Chính xác hơn: xóa theo danh sách citizen_hash của tập seed này theo từng lô
    for start in range(0, len(patients), batch_size):
        hashes_sql = ", ".join(sql_str(p[0]) for p in patients[start : start + batch_size])
        statements.append(f"DELETE FROM vaccination_records WHERE citizen_hash IN ({hashes_sql});")

    for start in range(0, len(vaccinations), batch_size):
        chunk = vaccinations[start : start + batch_size]
        select_unions = "\nUNION ALL\n".join(
            f"SELECT {sql_str(ch)} AS ch, {sql_str(cc)} AS cc, {sql_str(vt)} AS vt, "
            f"{dose} AS dn, {sql_str(bn)} AS bn, {sql_str(vd)} AS vd"
            for ch, cc, vt, dose, bn, vd in chunk
        )
        statements.append(
            f"""
            INSERT INTO vaccination_records (
                patient_id, citizen_hash, center_id, country_code,
                vaccine_type, dose_number, batch_number, vaccination_date
            )
            SELECT
                p.patient_id,
                v.ch,
                c.center_id,
                'VNM',
                v.vt,
                v.dn,
                v.bn,
                v.vd
            FROM (
            {select_unions}
            ) AS v
            JOIN patients AS p ON p.citizen_hash = v.ch
            JOIN vaccination_centers AS c ON c.center_code = v.cc;
            """
        )

    return statements, len(patients), len(vaccinations)


def execute_via_docker(sql_script: str, container: str, db_name: str, root_password: str) -> None:
    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "mysql",
        "--default-character-set=utf8mb4",
        "-uroot",
        f"-p{root_password}",
        db_name,
    ]
    res = subprocess.run(
        cmd,
        input=sql_script,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if res.returncode != 0:
        raise RuntimeError(f"MySQL seed qua Docker thất bại:\n{res.stderr}")


def main():
    parser = argparse.ArgumentParser(
        description="Sinh dữ liệu bệnh nhân và tiêm chủng giả lập cho MySQL OLTP (CDC -> DWH)."
    )
    parser.add_argument("--patients", type=int, default=2500, help="Số lượng bệnh nhân giả lập (mặc định: 2500)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed cố định để tái lập kết quả")
    parser.add_argument("--container", default="mysql-oltp", help="Tên container MySQL OLTP")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "vaccination_oltp"))
    parser.add_argument("--root-password", default=os.getenv("MYSQL_ROOT_PASSWORD", "root_password"))
    args = parser.parse_args()

    if args.patients < 1:
        raise ValueError("--patients phải >= 1")

    print(f"[*] Đang sinh SQL giả lập cho {args.patients} bệnh nhân (seed={args.seed})...")
    statements, total_patients, total_vax = generate_sql_batches(args.patients, args.seed)
    sql_script = "\n".join(statements)

    print(
        f"[*] Đang nạp {total_patients} bệnh nhân và {total_vax} mũi tiêm vào container "
        f"'{args.container}' ({args.database})..."
    )
    execute_via_docker(sql_script, args.container, args.database, args.root_password)
    print(
        f"[+] Hoàn tất! Đã nạp {total_patients} bệnh nhân và {total_vax} mũi tiêm vào MySQL OLTP.\n"
        "    Debezium + Kafka CDC sẽ tự động đồng bộ bản ghi ẩn danh sang dwh.dim_patient và dwh.fact_vaccination."
    )


if __name__ == "__main__":
    main()
