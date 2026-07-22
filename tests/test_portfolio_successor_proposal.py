from __future__ import annotations

from pathlib import Path

import pytest

import portfolio_successor_proposal as successor


def test_proposal_rebuilds_exhaustion_without_activation() -> None:
    proposal = successor.build_proposal()
    assert proposal["objective"] == "FIRST_PILOT_READY_LIVE_STARTED"
    assert proposal["authorization_state"] == "REQUIRED_NOT_GRANTED"
    assert proposal["campaign_activation_permitted"] is False
    assert proposal["candidate_preregistration_permitted"] is False
    assert proposal["provider_access_permitted"] is False
    assert proposal["outcome_access_permitted"] is False
    assert proposal["broker_actions_permitted"] is False
    assert proposal["current_campaign_exhaustion"]["pilot_ready_strategies"] == 0
    assert proposal["current_campaign_exhaustion"]["live_started_strategies"] == 0
    assert len(proposal["candidate_themes"]) == 3
    assert all(
        item["state"] == "THEME_ONLY_NOT_PREREGISTERED"
        for item in proposal["candidate_themes"]
    )


def test_content_addressed_proposal_audits_and_detects_mutation(
    tmp_path: Path,
) -> None:
    path, proposal = successor.write_proposal(tmp_path)
    audit = successor.audit_proposal(path)
    assert audit["valid"] is True
    assert audit["proposal_sha256"] == proposal["proposal_sha256"]
    changed = dict(proposal)
    changed["campaign_activation_permitted"] = True
    path.write_text(successor.json.dumps(changed), encoding="utf-8")
    with pytest.raises(successor.PortfolioSuccessorProposalError):
        successor.audit_proposal(path)
