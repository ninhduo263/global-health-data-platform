import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from kafka import KafkaConsumer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("vaccination-cdc-consumer")

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083").rstrip("/")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "vaccination-oltp-connector")
CONNECTOR_CONFIG_PATH = Path(
    os.getenv("DEBEZIUM_CONNECTOR_CONFIG", "/app/register-mysql-connector.json")
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PREFIX = os.getenv("DEBEZIUM_TOPIC_PREFIX", "health_cdc")
SOURCE_DATABASE = os.getenv("MYSQL_DATABASE", "vaccination_oltp")
MYSQL_CDC_USER = os.getenv("MYSQL_CDC_USER", "oltp_user")
MYSQL_CDC_PASSWORD = os.getenv("MYSQL_CDC_PASSWORD", "oltp_password")
DWH_CONFIG = {
    "host": os.getenv("DWH_HOST", "postgres-dwh"),
    "port": int(os.getenv("DWH_PORT", "5432")),
    "dbname": os.getenv("DWH_DB", "health_dwh"),
    "user": os.getenv("DWH_USER", "dwh_admin"),
    "password": os.getenv("DWH_PASSWORD", "dwh_password"),
    "connect_timeout": 10,
}


def ensure_connector() -> None:
    config = json.loads(CONNECTOR_CONFIG_PATH.read_text(encoding="utf-8"))["config"]
    config["database.user"] = MYSQL_CDC_USER
    config["database.password"] = MYSQL_CDC_PASSWORD
    config["database.include.list"] = SOURCE_DATABASE
    config["table.include.list"] = (
        f"{SOURCE_DATABASE}.vaccination_centers,"
        f"{SOURCE_DATABASE}.vaccination_records"
    )
    deadline = time.monotonic() + 300

    while time.monotonic() < deadline:
        try:
            connector_url = f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}"
            response = requests.get(connector_url, timeout=5)
            connector_updated = False
            if response.status_code == 404:
                response = requests.post(
                    f"{DEBEZIUM_URL}/connectors",
                    json={"name": CONNECTOR_NAME, "config": config},
                    timeout=10,
                )
                if response.status_code not in (200, 201):
                    response.raise_for_status()
                logger.info("Registered Debezium connector %s", CONNECTOR_NAME)
                connector_updated = True
            else:
                response.raise_for_status()
                connector_info = response.json()
                current_config = connector_info.get("config", connector_info)
                changed_keys = {
                    key
                    for key, value in config.items()
                    if key != "database.password" and current_config.get(key) != value
                }
                configured_password = current_config.get("database.password")
                if configured_password not in (
                    None,
                    "",
                    config["database.password"],
                    "[hidden]",
                    "********",
                ):
                    changed_keys.add("database.password")
                if changed_keys:
                    response = requests.put(
                        f"{connector_url}/config", json=config, timeout=10
                    )
                    response.raise_for_status()
                    logger.info(
                        "Updated Debezium connector %s configuration: %s",
                        CONNECTOR_NAME,
                        ", ".join(sorted(changed_keys)),
                    )
                    connector_updated = True

            if connector_updated:
                time.sleep(5)
                continue

            status = requests.get(f"{connector_url}/status", timeout=5)
            if status.ok:
                connector_status = status.json().get("connector", {}).get("state")
                task_states = [
                    task.get("state") for task in status.json().get("tasks", [])
                ]
                if connector_status == "RUNNING" and task_states and all(
                    state == "RUNNING" for state in task_states
                ):
                    logger.info("Debezium connector is running")
                    return
                if connector_status == "FAILED" or "FAILED" in task_states:
                    raise RuntimeError(f"Debezium connector failed: {status.text}")
        except (requests.RequestException, ValueError) as exc:
            logger.info("Waiting for Debezium Connect: %s", exc)

        time.sleep(5)

    raise TimeoutError("Debezium connector did not become ready within 5 minutes")


def decode_message(raw_value):
    if raw_value is None:
        return None
    value = json.loads(raw_value.decode("utf-8"))
    return value.get("payload", value)


def decode_key(raw_key):
    if raw_key is None:
        return {}
    key = json.loads(raw_key.decode("utf-8"))
    return key.get("payload", key)


def connect_to_dwh():
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            return psycopg2.connect(**DWH_CONFIG)
        except psycopg2.OperationalError as exc:
            logger.info("Waiting for DWH: %s", exc)
            time.sleep(3)
    raise TimeoutError("PostgreSQL DWH did not become available within 2 minutes")


def upsert_date(cursor, raw_date) -> int:
    if isinstance(raw_date, datetime):
        parsed_date = raw_date.date()
    elif isinstance(raw_date, date):
        parsed_date = raw_date
    elif isinstance(raw_date, int) or (
        isinstance(raw_date, str) and raw_date.isdigit()
    ):
        parsed_date = date(1970, 1, 1) + timedelta(days=int(raw_date))
    else:
        parsed_date = date.fromisoformat(str(raw_date)[:10])

    date_id = int(parsed_date.strftime("%Y%m%d"))
    cursor.execute(
        """
        INSERT INTO dwh.dim_date (date_id, full_date, year, month, day, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
        """,
        (
            date_id,
            parsed_date,
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            parsed_date.isoweekday() in (6, 7),
        ),
    )
    return date_id


def apply_center_event(cursor, operation, row) -> None:
    center_id = row.get("center_id")
    if center_id is None:
        raise ValueError("CDC center event is missing center_id")

    if operation == "d":
        cursor.execute(
            "DELETE FROM dwh.dim_vaccination_center WHERE id = %s", (center_id,)
        )
        return

    cursor.execute(
        """
        INSERT INTO dwh.dim_vaccination_center (
            id, center_code, center_name, province, country_code,
            is_placeholder, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, FALSE, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE SET
            center_code = EXCLUDED.center_code,
            center_name = EXCLUDED.center_name,
            province = EXCLUDED.province,
            country_code = EXCLUDED.country_code,
            is_placeholder = FALSE,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            center_id,
            row.get("center_code"),
            row["center_name"],
            row["province"],
            row.get("country_code") or "VNM",
        ),
    )


def apply_record_event(cursor, operation, row, key) -> None:
    record_id = (row or {}).get("record_id") or key.get("record_id")
    if record_id is None:
        raise ValueError("CDC vaccination record event is missing record_id")

    if operation == "d":
        cursor.execute(
            "DELETE FROM dwh.fact_vaccination WHERE record_id = %s", (record_id,)
        )
        return

    center_id = row.get("center_id")
    if center_id is None:
        raise ValueError(f"Vaccination record {record_id} is missing center_id")

    cursor.execute(
        """
        INSERT INTO dwh.dim_vaccination_center (
            id, center_name, province, country_code, is_placeholder
        )
        VALUES (%s, 'Pending CDC center metadata', 'Unknown', 'VNM', TRUE)
        ON CONFLICT (id) DO NOTHING
        """,
        (center_id,),
    )
    date_id = upsert_date(cursor, row["vaccination_date"])
    cursor.execute(
        """
        INSERT INTO dwh.fact_vaccination (
            record_id, center_id, date_id, vaccine_type, dose_number, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (record_id) DO UPDATE SET
            center_id = EXCLUDED.center_id,
            date_id = EXCLUDED.date_id,
            vaccine_type = EXCLUDED.vaccine_type,
            dose_number = EXCLUDED.dose_number,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            record_id,
            center_id,
            date_id,
            row["vaccine_type"],
            row["dose_number"],
        ),
    )


def apply_event(connection, topic, raw_key, raw_value) -> None:
    if raw_value is None:
        return

    envelope = decode_message(raw_value)
    if not envelope:
        return
    key = decode_key(raw_key)
    operation = envelope.get("op")
    source = envelope.get("source") or {}
    table_name = source.get("table") or topic.rsplit(".", 1)[-1]

    if operation == "t":
        with connection.cursor() as cursor:
            if table_name == "vaccination_records":
                cursor.execute("TRUNCATE TABLE dwh.fact_vaccination")
            elif table_name == "vaccination_centers":
                cursor.execute("DELETE FROM dwh.dim_vaccination_center")
        connection.commit()
        return

    row = envelope.get("after")
    if operation == "d":
        row = envelope.get("before") or {}
    if operation not in ("c", "u", "r", "d"):
        logger.warning("Ignoring unsupported Debezium operation %r on %s", operation, topic)
        return

    try:
        with connection.cursor() as cursor:
            if table_name == "vaccination_centers":
                center_row = row or key
                apply_center_event(cursor, operation, center_row)
            elif table_name == "vaccination_records":
                apply_record_event(cursor, operation, row, key)
            else:
                logger.warning("Ignoring unexpected Debezium topic %s", topic)
                return
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def main() -> None:
    ensure_connector()
    connection = connect_to_dwh()
    topic_pattern = re.compile(
        rf"^{re.escape(TOPIC_PREFIX)}\.{re.escape(SOURCE_DATABASE)}\."
        r"(vaccination_centers|vaccination_records)$"
    )
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="health-dwh-vaccination-consumer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=-1,
    )
    consumer.subscribe(pattern=topic_pattern)
    logger.info("Listening for vaccination CDC topics matching %s", topic_pattern.pattern)

    try:
        for message in consumer:
            apply_event(connection, message.topic, message.key, message.value)
            consumer.commit()
            logger.info(
                "Applied CDC event from %s at partition=%s offset=%s",
                message.topic,
                message.partition,
                message.offset,
            )
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
