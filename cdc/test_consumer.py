import json
import sys
import tempfile
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault("psycopg2", types.SimpleNamespace(OperationalError=OSError))
sys.modules.setdefault(
    "requests",
    types.SimpleNamespace(get=None, post=None, put=None, RequestException=Exception),
)
sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=object))

import consumer


class FakeCursor:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))


class FakeConnection:
    def __init__(self):
        self.fake_cursor = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def debezium_json(payload):
    return json.dumps({"schema": {}, "payload": payload}).encode()


class ConsumerTests(unittest.TestCase):
    def test_existing_connector_config_is_reconciled_once(self):
        connector_config = {
            "database.user": "oltp_user",
            "database.password": "oltp_password",
            "database.server.id": "184054",
            "database.include.list": "vaccination_oltp",
            "table.include.list": (
                "vaccination_oltp.vaccination_centers,"
                "vaccination_oltp.vaccination_records"
            ),
        }
        current_config = {
            **connector_config,
            "database.user": "root",
        }

        class Response:
            status_code = 200
            ok = True
            text = ""

            def __init__(self, payload):
                self.payload = payload

            def json(self):
                return self.payload

            def raise_for_status(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "connector.json"
            config_path.write_text(
                json.dumps({"config": connector_config}), encoding="utf-8"
            )

            def get(url, timeout):
                if url.endswith("/status"):
                    return Response(
                        {
                            "connector": {"state": "RUNNING"},
                            "tasks": [{"state": "RUNNING"}],
                        }
                    )
                return Response({"config": current_config})

            def put(url, json, timeout):
                current_config.update(json)
                return Response(json)

            with (
                patch.object(consumer, "CONNECTOR_CONFIG_PATH", config_path),
                patch.object(consumer, "MYSQL_CDC_USER", "oltp_user"),
                patch.object(consumer, "MYSQL_CDC_PASSWORD", "oltp_password"),
                patch.object(consumer.requests, "get", side_effect=get, create=True),
                patch.object(
                    consumer.requests, "put", side_effect=put, create=True
                ) as put_mock,
                patch.object(consumer.time, "sleep"),
            ):
                consumer.ensure_connector()

        self.assertEqual(put_mock.call_count, 1)

    def test_decode_debezium_payload_and_key(self):
        self.assertEqual(
            consumer.decode_message(debezium_json({"op": "r"})),
            {"op": "r"},
        )
        self.assertEqual(
            consumer.decode_key(debezium_json({"record_id": 42})),
            {"record_id": 42},
        )

    def test_upsert_date_decodes_debezium_epoch_days(self):
        cursor = FakeCursor()

        date_id = consumer.upsert_date(cursor, 18854)

        self.assertEqual(date_id, 20210815)
        self.assertEqual(cursor.statements[0][1][1], date(2021, 8, 15))

    def test_record_snapshot_upserts_aggregate_fact_without_pii(self):
        connection = FakeConnection()
        row = {
            "record_id": 42,
            "center_id": 7,
            "vaccine_type": "Pfizer-BioNTech",
            "dose_number": 1,
            "vaccination_date": 18854,
            "citizen_hash": "must-not-be-copied-to-dwh",
        }

        consumer.apply_event(
            connection,
            "health_cdc.vaccination_oltp.vaccination_records",
            debezium_json({"record_id": 42}),
            debezium_json(
                {
                    "op": "r",
                    "source": {"table": "vaccination_records"},
                    "after": row,
                }
            ),
        )

        statements = [statement for statement, _ in connection.fake_cursor.statements]
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        self.assertTrue(any("INSERT INTO dwh.fact_vaccination" in s for s in statements))
        self.assertTrue(
            any("INSERT INTO dwh.dim_vaccination_center" in s for s in statements)
        )
        self.assertFalse(any("citizen_hash" in s for s in statements))

    def test_center_delete_uses_primary_key_when_before_image_is_missing(self):
        connection = FakeConnection()

        consumer.apply_event(
            connection,
            "health_cdc.vaccination_oltp.vaccination_centers",
            debezium_json({"center_id": 7}),
            debezium_json(
                {
                    "op": "d",
                    "source": {"table": "vaccination_centers"},
                    "before": None,
                }
            ),
        )

        self.assertEqual(connection.commits, 1)
        self.assertEqual(
            connection.fake_cursor.statements[0][1],
            (7,),
        )

    def test_failed_dwh_write_rolls_back_without_committing(self):
        class FailingCursor(FakeCursor):
            def execute(self, statement, parameters=None):
                raise RuntimeError("simulated DWH failure")

        class FailingConnection(FakeConnection):
            def cursor(self):
                return FailingCursor()

        connection = FailingConnection()

        with self.assertRaisesRegex(RuntimeError, "simulated DWH failure"):
            consumer.apply_event(
                connection,
                "health_cdc.vaccination_oltp.vaccination_centers",
                debezium_json({"center_id": 7}),
                debezium_json(
                    {
                        "op": "r",
                        "source": {"table": "vaccination_centers"},
                        "after": {
                            "center_id": 7,
                            "center_code": "VN-HN-01",
                            "center_name": "Center",
                            "province": "Hanoi",
                            "country_code": "VNM",
                        },
                    }
                ),
            )

        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)


if __name__ == "__main__":
    unittest.main()
