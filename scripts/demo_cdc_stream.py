"""
Script Demo: Mo phong phat sinh ho so benh nhan va giao dich tiem chung theo thoi gian thuc (CDC Simulation)
Tu dong INSERT benh nhan moi + mui tiem moi vao MySQL OLTP
va kiem tra du lieu xuat hien tuc thi trong PostgreSQL DWH (dim_patient an danh & fact_vaccination)
qua Debezium + Kafka.
"""

import os
import random
import sys
import time
from datetime import date

import psycopg2
import pymysql

# Tu dong nhan dien moi truong (trong container Docker hoac tu may Windows Host)
IN_DOCKER = os.path.exists("/.dockerenv") or os.environ.get("DWH_HOST") == "postgres-dwh"

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql-oltp" if IN_DOCKER else "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306" if IN_DOCKER else "3307"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root_password")
MYSQL_DB = os.getenv("MYSQL_DB", "vaccination_oltp")

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh" if IN_DOCKER else "localhost")
DWH_PORT = int(os.getenv("DWH_PORT", "5432" if IN_DOCKER else "5433"))
DWH_USER = os.getenv("DWH_USER", "dwh_admin")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")
DWH_DB = os.getenv("DWH_DB", "health_dwh")

VACCINES = ["Pfizer-BioNTech", "AstraZeneca", "Moderna", "VeroCell"]
CENTERS = [1, 2, 3]
PROVINCES = ["Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng"]
STATUSES = ["HEALTHY", "SUSPECTED", "CONFIRMED_COVID", "RECOVERED"]
COMORBIDITIES = ["None", "Hypertension", "Diabetes", "Cardiovascular"]


def run_demo(iterations=3, delay_seconds=3):
    print("=" * 76)
    print("  KICH BAN DEMO CDC STREAMING: MySQL -> Debezium -> Kafka -> Postgres DWH")
    print("=" * 76)

    try:
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset="utf8mb4",
            autocommit=True,
        )
        print(f"[+] Ket noi thanh cong toi MySQL OLTP ({MYSQL_HOST}:{MYSQL_PORT})")
    except Exception as e:
        print(f"[-] Khong the ket noi toi MySQL OLTP: {e}")
        return

    try:
        pg_conn = psycopg2.connect(
            host=DWH_HOST,
            port=DWH_PORT,
            user=DWH_USER,
            password=DWH_PASSWORD,
            dbname=DWH_DB,
        )
        pg_conn.autocommit = True
        print(f"[+] Ket noi thanh cong toi PostgreSQL DWH ({DWH_HOST}:{DWH_PORT})")
    except Exception as e:
        print(f"[-] Khong the ket noi toi PostgreSQL DWH: {e}")
        mysql_conn.close()
        return

    print("-" * 76)
    print(f"Bat dau phat sinh {iterations} cap benh nhan + mui tiem moi (delay {delay_seconds}s)...")
    print("-" * 76)

    try:
        for i in range(1, iterations + 1):
            suffix = f"{int(time.time() * 1000) % 1_000_000}_{random.randint(100, 999)}"
            citizen_hash = f"demo_citizen_{suffix}"
            full_name = f"Demo Patient {suffix}"
            birth_year = random.randint(1955, 2005)
            gender = random.choice(["M", "F"])
            province = random.choice(PROVINCES)
            health_status = random.choice(STATUSES)
            comorbidity = random.choice(COMORBIDITIES)
            center_id = random.choice(CENTERS)
            vaccine = random.choice(VACCINES)
            dose = random.choice([1, 2, 3])
            batch = f"LOT-DEMO-{random.randint(100, 999)}"
            today_str = date.today().strftime("%Y-%m-%d")

            # 1. Ghi nhan ho so benh nhan + giao dich tiem chung vao MySQL OLTP
            with mysql_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO patients (
                        citizen_hash, full_name, birth_year, gender, province,
                        country_code, health_status, comorbidity, reported_date
                    ) VALUES (%s, %s, %s, %s, %s, 'VNM', %s, %s, %s);
                    """,
                    (
                        citizen_hash,
                        full_name,
                        birth_year,
                        gender,
                        province,
                        health_status,
                        comorbidity,
                        today_str,
                    ),
                )
                patient_id = cur.lastrowid

                cur.execute(
                    """
                    INSERT INTO vaccination_records (
                        patient_id, citizen_hash, center_id, country_code,
                        vaccine_type, dose_number, batch_number, vaccination_date
                    ) VALUES (%s, %s, %s, 'VNM', %s, %s, %s, %s);
                    """,
                    (patient_id, citizen_hash, center_id, vaccine, dose, batch, today_str),
                )
                record_id = cur.lastrowid

            print(
                f"[{i}/{iterations}] [MySQL OLTP] Benh nhan ID={patient_id} ({full_name}, {birth_year}, {comorbidity}) "
                f"| Record ID={record_id} ({vaccine}, Mui {dose})"
            )

            # Cho CDC Debezium & Kafka truyen tin
            time.sleep(delay_seconds)

            # 2. Kiem tra dim_patient (da an danh) va fact_vaccination trong PostgreSQL DWH
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute(
                    """
                    SELECT patient_id, birth_year, gender, province, health_status,
                           comorbidity, is_placeholder
                    FROM dwh.dim_patient
                    WHERE patient_id = %s;
                    """,
                    (patient_id,),
                )
                patient_row = pg_cur.fetchone()

                pg_cur.execute(
                    """
                    SELECT record_id, center_id, patient_id, vaccine_type, dose_number, updated_at
                    FROM dwh.fact_vaccination
                    WHERE record_id = %s;
                    """,
                    (record_id,),
                )
                vax_row = pg_cur.fetchone()

                if patient_row and not patient_row[6]:
                    print(
                        f"       -> [CDC -> dim_patient] DONG BO AN DANH THANH CONG! "
                        f"patient_id={patient_row[0]}, nam_sinh={patient_row[1]}, "
                        f"gioi_tinh={patient_row[2]}, benh_nen={patient_row[5]} (khong luu PII)"
                    )
                else:
                    print("       -> [CDC -> dim_patient] Dang doi Kafka chuyen ban tin benh nhan...")

                if vax_row:
                    print(
                        f"       -> [CDC -> fact_vaccination] DONG BO THANH CONG! "
                        f"record_id={vax_row[0]}, patient_id={vax_row[2]}, "
                        f"vaccine={vax_row[3]}, mui={vax_row[4]}"
                    )
                else:
                    print("       -> [CDC -> fact_vaccination] Dang doi Kafka chuyen ban tin mui tiem...")

            print()

        print("=" * 76)
        print("  HOAN TAT DEMO CDC STREAMING REALTIME THANH CONG!")
        print("=" * 76)
    finally:
        mysql_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    run_demo(iterations=count, delay_seconds=3)
