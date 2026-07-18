import tempfile
import unittest
from pathlib import Path

from learning_registry import (
    RegistryError,
    append_event,
    audit_registries,
    current_entities,
    load_registry,
)


def dataset_event(event_id="dataset-test-registered", event_type="registered"):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "entity_id": "dataset-test",
        "event_type": event_type,
        "recorded_at": "2026-07-18T17:00:00-04:00",
        "payload": {
            "lane": "development",
            "status": "READY",
            "evidence_paths": ["evidence/test.json"],
            "inspected": True,
        },
    }


class LearningRegistryTests(unittest.TestCase):
    def test_append_and_current_entity_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            append_event("datasets", dataset_event(), root)
            update = dataset_event("dataset-test-status", "status")
            update["payload"]["status"] = "RETIRED"
            append_event("datasets", update, root)

            self.assertEqual(len(load_registry("datasets", root)), 2)
            self.assertEqual(
                current_entities("datasets", root)["dataset-test"]["payload"]["status"],
                "RETIRED",
            )

    def test_duplicate_or_pre_registration_change_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            append_event("datasets", dataset_event(), root)
            with self.assertRaisesRegex(RegistryError, "duplicate event_id"):
                append_event("datasets", dataset_event(), root)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RegistryError, "not registered"):
                append_event(
                    "datasets", dataset_event("dataset-test-status", "status"), root
                )

    def test_corrupt_or_unsafe_registry_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.mkdir(exist_ok=True)
            (root / "DATASETS.jsonl").write_text("not-json\n", encoding="utf-8")
            with self.assertRaisesRegex(RegistryError, "invalid JSON"):
                load_registry("datasets", root)
        event = dataset_event()
        event["payload"]["evidence_paths"] = ["../private.json"]
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RegistryError, "repository relative"):
                append_event("datasets", event, Path(directory))

    def test_repository_registries_audit(self):
        result = audit_registries()

        self.assertTrue(result["valid"])
        self.assertEqual(result["datasets"]["entities"], 3)
        self.assertEqual(result["experiments"]["entities"], 2)
        self.assertEqual(result["strategies"]["entities"], 2)


if __name__ == "__main__":
    unittest.main()
