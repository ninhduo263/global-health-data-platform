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
    types.SimpleNamespace(
        get=None, post=None, put=None, delete=None, RequestException=Exception
    ),
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
            "database.server.id": "184055",
            "database.include.list": "vaccination_oltp",
            "table.include.list": (
                "vaccination_oltp.vaccination_centers,"
                "vaccination_oltp.patients,"
                "vaccination_oltp.vaccination_records"
            ),
        }
        current_config = {
            **connector_config,
            "database.user": "root",
        }

        class Response:
            ok = True
            text = ""

            def __init__(self, payload, status_code=200):
                self.payload = payload
                self.status_code = status_code

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

            def delete(url, timeout):
                return Response({}, status_code=404)

            with (
                patch.object(consumer, "CONNECTOR_CONFIG_PATH", config_path),
                patch.object(consumer, "MYSQL_CDC_USER", "oltp_user"),
                patch.object(consumer, "MYSQL_CDC_PASSWORD", "oltp_password"),
                patch.object(consumer.requests, "get", side_effect=get, create=True),
                patch.object(
                    consumer.requests, "put", side_effect=put, create=True
                ) as put_mock,
                patch.object(
                    consumer.requests, "delete", side_effect=delete, create=True
                ),
                patch.object(consumer.time, "sleep"),
            ):
                consumer.ensure_connector()

        self.assertEqual(put_mock.call_count, 1)
        self.assertIn("vaccination_oltp.patients", current_config["table.include.list"])

    def test_legacy_connector_is_removed_but_current_one_is_kept(self):
        class Response:
            def __init__(self, status_code):
                self.status_code = status_code

            def raise_for_status(self):
                return None

        with (
            patch.object(consumer, "CONNECTOR_NAME", "vaccination-oltp-connector-v2"),
            patch.object(
                consumer,
                "LEGACY_CONNECTOR_NAMES",
                ("vaccination-oltp-connector", "vaccination-oltp-connector-v2"),
            ),
            patch.object(
                consumer.requests,
                "delete",
                return_value=Response(204),
                create=True,
            ) as delete_mock,
        ):
            consumer.remove_legacy_connectors()

        self.assertEqual(delete_mock.call_count, 1)
        self.assertTrue(
            delete_mock.call_args.args[0].endswith("/connectors/vaccination-oltp-connector")
        )

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

    def test_patient_snapshot_is_anonymized_in_dwh(self):
        connection = FakeConnection()
        row = {
            "patient_id": 5,
            "citizen_hash": "a1b2c3d4e5f60005",
            "full_name": "Hoang Thu Ha",
            "birth_year": 1998,
            "gender": "F",
            "province": "Đà Nẵng",
            "country_code": "VNM",
            "health_status": "RECOVERED",
            "comorbidity": "None",
            "reported_date": 18856,
        }

        consumer.apply_event(
            connection,
            "health_cdc.vaccination_oltp.patients",
            debezium_json({"patient_id": 5}),
            debezium_json(
                {"op": "r", "source": {"table": "patients"}, "after": row}
            ),
        )

        patient_inserts = [
            (statement, parameters)
            for statement, parameters in connection.fake_cursor.statements
            if "INSERT INTO dwh.dim_patient" in statement
        ]
        self.assertEqual(connection.commits, 1)
        self.assertEqual(len(patient_inserts), 1)
        statement, parameters = patient_inserts[0]
        self.assertNotIn("full_name", statement)
        self.assertNotIn("citizen_hash", statement)
        self.assertNotIn("Hoang Thu Ha", parameters)
        self.assertNotIn("a1b2c3d4e5f60005", parameters)
        self.assertEqual(parameters[0], 5)
        self.assertEqual(parameters[-1], 20210817)

    def test_patient_delete_uses_key_when_before_image_is_missing(self):
        connection = FakeConnection()

        consumer.apply_event(
            connection,
            "health_cdc.vaccination_oltp.patients",
            debezium_json({"patient_id": 9}),
            debezium_json({"op": "d", "source": {"table": "patients"}, "before": None}),
        )

        statement, parameters = connection.fake_cursor.statements[0]
        self.assertIn("DELETE FROM dwh.dim_patient", statement)
        self.assertEqual(parameters, (9,))

    def test_record_links_patient_and_creates_placeholder(self):
        connection = FakeConnection()
        row = {
            "record_id": 43,
            "patient_id": 5,
            "center_id": 1,
            "vaccine_type": "Moderna",
            "dose_number": 2,
            "vaccination_date": 18854,
        }

        consumer.apply_event(
            connection,
            "health_cdc.vaccination_oltp.vaccination_records",
            debezium_json({"record_id": 43}),
            debezium_json(
                {"op": "c", "source": {"table": "vaccination_records"}, "after": row}
            ),
        )

        statements = connection.fake_cursor.statements
        placeholder = [
            parameters
            for statement, parameters in statements
            if "INSERT INTO dwh.dim_patient" in statement
        ]
        fact = [
            parameters
            for statement, parameters in statements
            if "INSERT INTO dwh.fact_vaccination" in statement
        ]
        self.assertEqual(placeholder, [(5,)])
        self.assertEqual(fact[0][:3], (43, 1, 5))

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
