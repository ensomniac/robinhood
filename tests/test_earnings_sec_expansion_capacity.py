import hashlib
from pathlib import Path

import earnings_sec_expansion_capacity as source
import earnings_sec_expansion_capacity_inspection as inspection


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )


def _rolling_bypass(monkeypatch):
    monkeypatch.setattr(
        source,
        "rolling_authority",
        lambda: {
            "authorization_path": "authority.json",
            "authorization_file_sha256": "a" * 64,
            "authorization_sha256": "b" * 64,
            "status_path": "status.json",
            "status_file_sha256": "c" * 64,
            "activation_policy": "ROLLING_TERMINAL_REPLACEMENT",
            "calendar_wait_required": False,
            "existing_mechanism_family": True,
            "new_mechanism_family_slot_consumed": False,
        },
    )


def test_contract_freezes_larger_historical_lane_without_calendar_wait(
    monkeypatch,
):
    _commit_bypass(monkeypatch)
    _rolling_bypass(monkeypatch)

    contract = source.build_contract(created_at="2026-07-25T07:20:00Z")

    assert len(contract["requests"]) == 32
    assert contract["requests"][0]["filename"] == "2012q1_notes.zip"
    assert contract["requests"][-1]["filename"] == "2019q4_notes.zip"
    assert contract["rolling_authority"]["calendar_wait_required"] is False
    assert (
        contract["rolling_authority"]["new_mechanism_family_slot_consumed"]
        is False
    )
    assert contract["partitions"]["development"] == [
        "2012-01-01",
        "2017-12-15",
    ]
    assert contract["partitions"]["confirmation"] == [
        "2018-01-08",
        "2019-12-31",
    ]
    assert contract["market_prices_accessed"] is False
    assert contract["provider_requests_executed"] == 0
    assert contract["broker_actions"] == 0


def test_request_graph_is_deterministic_and_hash_bound():
    first = source.requests()
    second = source.requests()

    assert first == second
    assert [row["ordinal"] for row in first] == list(range(32))
    assert len({row["request_sha256"] for row in first}) == 32
    for row in first:
        without_hash = {
            key: value
            for key, value in row.items()
            if key != "request_sha256"
        }
        assert row["request_sha256"] == hashlib.sha256(
            source.canonical_bytes(without_hash)
        ).hexdigest()


def test_independent_inspection_rebuilds_exact_zero_outcome_contract(
    monkeypatch, tmp_path: Path
):
    _commit_bypass(monkeypatch)
    _rolling_bypass(monkeypatch)
    monkeypatch.setattr(
        source.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "d" * 64},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "d" * 64},
    )
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    contract_path, contract = source.freeze_contract(
        created_at="2026-07-25T07:20:00Z", root=tmp_path
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )

    path, result = inspection.inspect_contract(
        contract_path,
        inspected_at="2026-07-25T07:21:00Z",
        root=tmp_path,
    )

    assert path.exists()
    assert result["valid"] is True
    assert all(result["checks"].values())
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["authorized_metadata_requests"] == 32
    assert result["market_price_access_authorized"] is False


def test_contract_self_hash_rejects_mutation(monkeypatch):
    _commit_bypass(monkeypatch)
    _rolling_bypass(monkeypatch)
    contract = source.build_contract(created_at="2026-07-25T07:20:00Z")
    original = contract["contract_sha256"]

    contract["partitions"]["development"][1] = "2017-12-16"

    assert source.self_hash(contract, "contract_sha256") != original
