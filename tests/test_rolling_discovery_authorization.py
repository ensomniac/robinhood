from __future__ import annotations

from pathlib import Path

import pytest

import rolling_discovery_authorization as authorization
import rolling_discovery_authorization_inspection as inspection


def test_rolling_policy_releases_terminal_slots_without_weakening_gates():
    value = authorization.build_authorization()

    assert value["activation_policy"] == "ROLLING_TERMINAL_REPLACEMENT"
    assert value["maximum_concurrent_active_mechanism_families"] == 3
    assert value["active_family_count"] == 0
    assert value["available_slot_count"] == 3
    assert len(value["terminal_predecessors"]) == 3
    assert all(value["selection_accounting"].values())
    assert value["preservation_contract"] == {
        "failed_corpus_parameter_repair_allowed": False,
        "confirmation_reuse_allowed": False,
        "evidence_or_promotion_gate_weakening_allowed": False,
        "risk_protection_or_broker_gate_weakening_allowed": False,
        "maximum_parallel_outcome_active_families": 3,
    }
    assert value["target_outcome_access_permitted_before_family_freeze"] is False
    assert value["broker_actions_permitted"] is False


def test_authorization_needs_independent_inspection(tmp_path: Path):
    root = tmp_path / "authorization"
    status = root / "status.json"
    path, value = authorization.write_authorization(root=root, status_path=status)

    with pytest.raises(authorization.RollingDiscoveryAuthorizationError):
        authorization.load_ready_status(status)

    ready = inspection.inspect(path, status_path=status)
    assert ready["state"] == "ROLLING_DISCOVERY_AUTHORIZED"
    assert ready["authorization_sha256"] == value["authorization_sha256"]
    assert ready["available_slot_count"] == 3
    assert ready["provider_access_permitted"] is True
    assert ready["target_outcome_access_permitted"] is False
    assert ready["broker_actions_permitted"] is False


def test_terminal_predecessor_mutation_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    changed = tmp_path / "changed.json"
    changed.write_text("{}\n", encoding="utf-8")
    bindings = dict(authorization.TERMINAL_BINDINGS)
    bindings["multi-asset-etf-tsmom-v1"] = changed
    monkeypatch.setattr(authorization, "TERMINAL_BINDINGS", bindings)

    with pytest.raises(
        authorization.RollingDiscoveryAuthorizationError,
        match="not terminal",
    ):
        authorization.build_authorization()
