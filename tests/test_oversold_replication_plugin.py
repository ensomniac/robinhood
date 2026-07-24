from __future__ import annotations

from pathlib import Path

import oversold_replication_plugin as plugin


def test_store_path_rejects_escape(tmp_path: Path) -> None:
    class Store:
        root = tmp_path

    assert plugin._store_path(Store(), "safe/input.json.gz") == (
        tmp_path / "safe/input.json.gz"
    )
    try:
        plugin._store_path(Store(), "../escaped.json.gz")
    except plugin.OversoldReplicationPluginError as exc:
        assert "escaped" in str(exc)
    else:
        raise AssertionError("private input path must not escape the store")


def test_preflight_does_not_open_private_inputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    evidence_path = tmp_path / "inspection.json"
    monkeypatch.setattr(plugin, "_repo_path", lambda value: Path(value))
    monkeypatch.setattr(plugin, "_require_committed", lambda _path: None)
    monkeypatch.setattr(
        plugin,
        "_manifest",
        lambda _path: {
            "requested_dates": ["2023-01-26"],
            "dataset_payload": {
                "lane": "development",
                "point_in_time_evidence": True,
                "evidence_paths": [str(evidence_path)],
                "oversold_replication_capacity": {
                    "family_id": "gap-universe-oversold-reversal",
                    "formal_capacity": 100,
                    "development_training_contaminated": True,
                    "confirmation_access_permitted": False,
                    "zero_signal_days": 299,
                },
            },
        },
    )
    def reject_private_open(_path):
        raise AssertionError("preflight opened a private input")

    monkeypatch.setattr(plugin, "_read_gzip", reject_private_open)
    result = plugin.preflight(
        {
            "capacity_manifest": str(manifest_path),
            "development_dates": ["2023-01-26"],
        }
    )
    assert result["verified_capacity"] == 100
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False


def test_production_delegates_exact_evaluator(monkeypatch) -> None:
    observed = {}

    def evaluate(winner, facts):
        observed["winner"] = winner
        observed["facts"] = facts
        return {"rules_hash": "rules"}

    monkeypatch.setattr(
        plugin.production,
        "evaluate_production",
        evaluate,
    )
    winner = {"rules_hash": "rules"}
    facts = {"quote": "fresh"}
    assert plugin.evaluate_production(winner, facts) == {
        "rules_hash": "rules"
    }
    assert observed == {"winner": winner, "facts": facts}
