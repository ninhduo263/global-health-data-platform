"""
Script Demo: Mo phong phat sinh giao dich tiem chung theo thoi gian thuc (CDC Simulation)
Tu dong INSERT ban ghi tiem chung moi vao MySQL OLTP
va kiem tra du lieu xuat hien tuc thi trong PostgreSQL DWH qua Debezium + Kafka.
"""

import os
import random
import sys
import time
from datetime import date

import pymysql
import psycopg2

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


def run_demo(iterations=3, delay_seconds=2):
    print("=" * 72)
    print("  KICH BAN DEMO CDC STREAMING: MySQL -> Debezium -> Kafka -> Postgres DWH")
    print("=" * 72)

    try:
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
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
        print(f"[+] Ket noi thanh cong toi PostgreSQL DWH ({DWH_HOST}:{DWH_PORT})")
    except Exception as e:
        print(f"[-] Khong the ket noi toi PostgreSQL DWH: {e}")
        mysql_conn.close()
        return

    print("-" * 72)
    print(f"Bat dau phat sinh {iterations} giao dich tiem chung moi (delay {delay_seconds}s)...")
    print("-" * 72)

    try:
        for i in range(1, iterations + 1):
            citizen_hash = f"demo_citizen_{random.randint(10000, 99999)}"
            center_id = random.choice(CENTERS)
            vaccine = random.choice(VACCINES)
            dose = random.choice([1, 2, 3])
            batch = f"LOT-DEMO-{random.randint(100, 999)}"
            today_str = date.today().strftime("%Y-%m-%d")

            # 1. Ghi nhan giao dich vao MySQL OLTP
            with mysql_conn.cursor() as cur:
                insert_sql = """
                INSERT INTO vaccination_records (
                    citizen_hash, center_id, country_code,
                    vaccine_type, dose_number, batch_number, vaccination_date
                ) VALUES (%s, %s, 'VNM', %s, %s, %s, %s);
                """
                cur.execute(insert_sql, (citizen_hash, center_id, vaccine, dose, batch, today_str))
                inserted_id = cur.lastrowid

            print(f"[{i}/{iterations}] [MySQL OLTP] Phat sinh mui tiem: Record ID={inserted_id} | Loai={vaccine} | Mui={dose} | Diem tiem={center_id}")

            # Cho CDC Debezium & Kafka truyen tin
            time.sleep(delay_seconds)

            # 2. Kiem tra xem ban ghi da sang PostgreSQL DWH chua
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute(
                    "SELECT record_id, center_id, vaccine_type, dose_number, updated_at FROM dwh.fact_vaccination WHERE record_id = %s;",
                    (inserted_id,),
                )
                row = pg_cur.fetchone()
                if row:
                    print(f"       -> [CDC -> DWH] DONG BO THANH CONG! Record ID={row[0]} da xuat hien trong dwh.fact_vaccination (Cap nhat: {row[4]})")
                else:
                    print(f"       -> [CDC -> DWH] Dang doi Kafka chuyen ban tin...")

            print()

        print("=" * 72)
        print("  HOAN TAT DEMO CDC STREAMING REALTIME THANH CONG!")
        print("=" * 72)
    finally:
        mysql_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    run_demo(iterations=count, delay_seconds=2)

