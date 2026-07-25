import hashlib
from pathlib import Path

import earnings_sec_corrected_expansion as source
import earnings_sec_corrected_expansion_inspection as inspection


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )


def _lineage_bypass(monkeypatch):
    monkeypatch.setattr(
        source,
        "_v12_failure_lineage",
        lambda: {
            "inspection_path": "failure-inspection.json",
            "inspection_file_sha256": "a" * 64,
            "inspection_sha256": "b" * 64,
            "state": "SEC_EXPANSION_ARCHIVE_ROOT_404_INSPECTED_TERMINAL",
            "v12_resume_permitted": False,
            "corrected_source_successor_permitted": True,
        },
    )
    monkeypatch.setattr(
        source.v12_capacity,
        "rolling_authority",
        lambda: {
            "authorization_path": "authority.json",
            "authorization_file_sha256": "c" * 64,
            "authorization_sha256": "d" * 64,
            "status_path": "status.json",
            "status_file_sha256": "e" * 64,
            "activation_policy": "ROLLING_TERMINAL_REPLACEMENT",
            "calendar_wait_required": False,
            "existing_mechanism_family": True,
            "new_mechanism_family_slot_consumed": False,
        },
    )


def test_corrected_contract_is_no_wait_outcome_blind_and_cumulative(
    monkeypatch,
):
    _commit_bypass(monkeypatch)
    _lineage_bypass(monkeypatch)

    contract = source.build_contract(created_at="2026-07-25T07:48:00Z")

    assert contract["archive_root"].endswith(
        "financial-statement-notes-data-sets"
    )
    assert "-and-" not in contract["archive_root"]
    assert len(contract["requests"]) == 16
    assert contract["requests"][0]["filename"] == "2012q1_notes.zip"
    assert contract["requests"][-1]["filename"] == "2015q4_notes.zip"
    assert contract["partitions"]["development"] == [
        "2012-01-01",
        "2014-12-19",
    ]
    assert contract["partitions"]["confirmation"] == [
        "2015-01-12",
        "2015-12-31",
    ]
    assert contract["rolling_authority"]["calendar_wait_required"] is False
    assert (
        contract["selection_accounting"][
            "cumulative_trial_count_if_full_search"
        ]
        == 64
    )
    assert contract["provider_requests_executed"] == 0
    assert contract["market_prices_accessed"] is False
    assert contract["broker_actions"] == 0


def test_corrected_request_graph_is_exact_and_hash_bound():
    rows = source.requests_graph()

    assert [row["ordinal"] for row in rows] == list(range(16))
    assert len({row["request_sha256"] for row in rows}) == 16
    for row in rows:
        content = {
            key: value
            for key, value in row.items()
            if key != "request_sha256"
        }
        assert row["request_sha256"] == hashlib.sha256(
            source.canonical_bytes(content)
        ).hexdigest()


def test_inspector_proves_hold_plus_five_session_embargo():
    assert inspection._embargo_is_sufficient() is True


def test_contract_inspection_rebuilds_exact_zero_outcome_boundary(
    monkeypatch, tmp_path: Path
):
    _commit_bypass(monkeypatch)
    _lineage_bypass(monkeypatch)
    monkeypatch.setattr(
        source.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "f" * 64},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "f" * 64},
    )
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    contract_path, contract = source.freeze_contract(
        created_at="2026-07-25T07:48:00Z", root=tmp_path
    )

    output, result = inspection.inspect_contract(
        contract_path,
        inspected_at="2026-07-25T07:49:00Z",
        root=tmp_path,
    )

    assert output.exists()
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["authorized_provider_requests"] == 16
    assert all(result["checks"].values())
    assert result["market_price_access_authorized"] is False


def test_plan_freezes_only_inspected_contract_graph(monkeypatch, tmp_path):
    _commit_bypass(monkeypatch)
    contract = {
        "contract_sha256": "a" * 64,
        "provider": "SEC",
        "requests": source.requests_graph(),
        "event_semantics": {
            "event_rank": [
                "descending EPS change ratio",
                "descending EPS absolute change",
                "canonical ticker",
                "accession",
            ]
        },
        "partitions": {
            "development": [source.DEVELOPMENT_START, source.DEVELOPMENT_END],
            "embargo": [source.EMBARGO_START, source.EMBARGO_END],
            "confirmation": [
                source.CONFIRMATION_START,
                source.CONFIRMATION_END,
            ],
        },
        "implementation_hashes": {"implementation": "b" * 64},
    }
    inspected = {"inspection_sha256": "c" * 64}
    monkeypatch.setattr(
        source,
        "_contract_lineage",
        lambda _contract, _inspection: (contract, inspected),
    )
    monkeypatch.setattr(source, "sha256_file", lambda _path: "d" * 64)
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))

    plan = source.build_plan(
        tmp_path / "contract.json",
        tmp_path / "inspection.json",
        created_at="2026-07-25T07:50:00Z",
    )

    assert plan["requests"] == source.requests_graph()
    assert plan["request_count"] == 16
    assert plan["transport"]["retries_permitted"] == 0
    assert plan["transport"]["substitutions_permitted"] == 0
    assert plan["market_prices_accessed"] is False
    assert plan["broker_actions"] == 0


def test_exposed_event_receives_zero_credit_without_substitution(monkeypatch):
    event = {
        "accepted": "2012-04-20 16:15:00",
        "ticker": "EDGE",
    }
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "read_index",
        lambda: [
            inspection.outcome_exposure.build_record(
                exposure_id="prior",
                campaign_id=source.CAMPAIGN_ID,
                lane="development",
                recorded_at="2026-07-25T07:00:00Z",
                source_path="prior.json",
                source_sha256="a" * 64,
                scope={"dates": ["2012-04-23"], "symbols": ["EDGE"]},
            )
        ],
    )

    admitted, exposed = inspection._scope_for_events(
        [event], ["2012-04-20", "2012-04-23"]
    )

    assert admitted == []
    assert exposed == 1
