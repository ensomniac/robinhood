from __future__ import annotations

from pathlib import Path

import pytest

import portfolio_successor_activation as activation
import portfolio_successor_activation_inspection as inspection


def test_authorization_preserves_exact_scope_and_closed_gates() -> None:
    value = activation.build_authorization()
    assert value["authorization_source"] == "EXPLICIT_USER_MESSAGE"
    assert value["authorization_text"] == activation.AUTHORIZATION_TEXT
    assert value["objective"] == "FIRST_PILOT_READY_LIVE_STARTED"
    assert value["scope"]["maximum_new_mechanism_families_per_iso_week"] == 3
    assert value["preservation_contract"] == {
        "v1_evidence_remains_adverse_and_immutable": True,
        "failed_corpus_parameter_repair_allowed": False,
        "prior_evidence_relabeling_allowed": False,
        "promotion_or_risk_gate_weakening_allowed": False,
    }
    assert value["candidate_preregistration_permitted_before_inspection"] is False
    assert value["provider_access_permitted_before_inspection"] is False
    assert value["outcome_access_permitted"] is False
    assert value["broker_actions_permitted"] is False


def test_frozen_authorization_requires_independent_inspection(tmp_path: Path) -> None:
    output = tmp_path / "authorizations"
    status = tmp_path / "status.json"
    path, value = activation.write_authorization(output, status)
    frozen = activation._read_json(status)
    assert frozen["status"] == "AUTHORIZED_PENDING_INSPECTION"
    assert frozen["candidate_preregistration_permitted"] is False
    ready = inspection.inspect_authorization(
        authorization_path=path, status_path=status
    )
    assert ready["status"] == "AUTHORIZED_READY"
    assert ready["candidate_preregistration_permitted"] is True
    assert ready["provider_access_permitted"] is False
    assert ready["outcome_access_permitted"] is False
    assert ready["broker_actions_permitted"] is False
    assert ready["authorization_sha256"] == value["authorization_sha256"]


def test_authorization_detects_mutation(tmp_path: Path) -> None:
    path, value = activation.write_authorization(
        tmp_path / "authorizations", tmp_path / "status.json"
    )
    changed = dict(value)
    changed["outcome_access_permitted"] = True
    path.write_text(activation.json.dumps(changed), encoding="utf-8")
    with pytest.raises(activation.PortfolioSuccessorActivationError):
        activation.load_authorization(path)
