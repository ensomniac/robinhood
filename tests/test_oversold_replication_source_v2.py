from __future__ import annotations

import json

import oversold_replication_source as base
import oversold_replication_source_v2 as source


def test_configuration_changes_only_identity_recovered_scanner_paths() -> None:
    original = {
        name: getattr(base, name)
        for name in (
            "DATASET_ID",
            "SCANNER_MANIFEST_ROOT",
            "SECURITY_MASTER",
            "SECURITY_SOURCE",
        )
    }

    with source.configured():
        assert base.DATASET_ID == source.DATASET_ID
        assert base.SCANNER_MANIFEST_ROOT == source.SCANNER_MANIFEST_ROOT
        assert base.SECURITY_MASTER == source.SECURITY_MASTER
        assert base.SECURITY_SOURCE == source.SECURITY_SOURCE
        assert base.SELECTION_PATH == source.ROOT / (
            "selection-2026-oversold-replication.json"
        )
        assert base.FRONT_EMBARGO_SESSIONS == 5

    for name, value in original.items():
        assert getattr(base, name) == value


def test_freeze_requires_identity_recovery_before_base_call(monkeypatch) -> None:
    called = False

    def fail_identity():
        raise source.OversoldReplicationSourceV2Error("identity not ready")

    def base_freeze(_env):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(source, "validate_identity_recovery", fail_identity)
    monkeypatch.setattr(base, "freeze_scanner", base_freeze)

    try:
        source.freeze_scanner(source.PROJECT_ROOT / ".env")
    except source.OversoldReplicationSourceV2Error as exc:
        assert "identity not ready" in str(exc)
    else:
        raise AssertionError("identity recovery must fail closed")
    assert called is False


def test_controller_binding_rejects_controller_drift(
    monkeypatch, tmp_path
) -> None:
    controller = tmp_path / "controller.py"
    base_controller = tmp_path / "base.py"
    selection = tmp_path / "selection.json"
    master = tmp_path / "master.jsonl"
    master_source = tmp_path / "master-source.json"
    master_inspection = tmp_path / "master-inspection.json"
    manifest = tmp_path / "manifest.json"
    binding = tmp_path / "binding.json"
    for path, value in (
        (controller, "v1"),
        (base_controller, "base"),
        (selection, "{}"),
        (master, "{}\n"),
        (master_source, "{}"),
        (master_inspection, "{}"),
        (manifest, json.dumps({"manifest_sha256": "manifest"})),
    ):
        path.write_text(value, encoding="utf-8")

    monkeypatch.setattr(source, "__file__", str(controller))
    monkeypatch.setattr(base, "__file__", str(base_controller))
    monkeypatch.setattr(source, "CONTROLLER_BINDING_PATH", binding)
    monkeypatch.setattr(source, "SECURITY_MASTER", master)
    monkeypatch.setattr(source, "SECURITY_SOURCE", master_source)
    monkeypatch.setattr(base, "SELECTION_PATH", selection)
    monkeypatch.setattr(source, "_master_inspection_path", lambda: master_inspection)
    monkeypatch.setattr(source, "_scanner_manifest_path", lambda: manifest)
    monkeypatch.setattr(base, "_repo_path", lambda path: str(path))

    frozen = source._build_controller_binding()
    binding.write_text(json.dumps(frozen), encoding="utf-8")
    assert source.validate_controller_binding(require_committed=False)[
        "exact_rebuild"
    ]

    controller.write_text("v2", encoding="utf-8")
    try:
        source.validate_controller_binding(require_committed=False)
    except source.OversoldReplicationSourceV2Error as exc:
        assert "missing or drifted" in str(exc)
    else:
        raise AssertionError("controller drift must fail closed")
