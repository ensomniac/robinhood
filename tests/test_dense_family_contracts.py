from __future__ import annotations

import json
import sys
from datetime import date, timedelta

import pytest

import dense_family_contracts as contracts
import next_week_discovery_batch as batch
import outcome_exposure
import strategy_discovery
from learning_data import freeze_dataset_contract


def _dates(start: date, count: int) -> list[str]:
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _capacity_manifest(tmp_path, family_id: str, requested_dates: list[str]):
    path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-capacity-{family_id}",
            "registered_at": "2026-07-23T08:00:00-04:00",
            "requested_dates": requested_dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["next_week_discovery_batch.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": family_id,
                    "formal_capacity": 120,
                },
            },
        },
        tmp_path / "capacity",
    )
    return path


def _inventory(tmp_path, index_path, *, overlap: bool = False):
    plan = batch.build_plan()
    symbols = {
        "liquid-equity-market-residual-reversal": ["AAPL"],
        "intraday-index-etf-opening-reversal": ["SPY"],
        "liquid-etf-trend-pullback-cost-floor": ["QQQ"],
    }
    families = []
    for ordinal, family in enumerate(plan["families"]):
        start = date(2018 + ordinal * 2, 1, 1)
        warmup_count = (
            60
            if family["family_id"] == "intraday-index-etf-opening-reversal"
            else 200
        )
        development_warmup = _dates(
            start - timedelta(days=warmup_count), warmup_count
        )
        development = _dates(start, 60)
        confirmation_start = (
            date(2025, 1, 1)
            if overlap and ordinal == 0
            else start + timedelta(days=65)
        )
        embargo = _dates(confirmation_start - timedelta(days=5), 5)
        confirmation = _dates(confirmation_start, 25)
        confirmation_warmup = _dates(
            date.fromisoformat(confirmation[0]) - timedelta(days=warmup_count),
            warmup_count,
        )
        requested = sorted(
            {
                *development_warmup,
                *development,
                *embargo,
                *confirmation_warmup,
                *confirmation,
            }
        )
        family_symbols = symbols[family["family_id"]]
        families.append(
            {
                "family_id": family["family_id"],
                "capacity_manifest": str(
                    _capacity_manifest(tmp_path, family["family_id"], requested)
                ),
                "development_warmup_dates": development_warmup,
                "confirmation_warmup_dates": confirmation_warmup,
                "development_dates": development,
                "embargo_dates": embargo,
                "confirmation_dates": confirmation,
                    "development_scope": {
                        "dates": development,
                        "symbols": family_symbols,
                    },
                    "confirmation_scope": {
                        "dates": confirmation,
                        "symbols": family_symbols,
                    },
                    "warmup_contract": {
                        "point_in_time_features_only": True,
                        "target_outcomes_eligible": False,
                        "prior_exposure_allowed": True,
                        "cross_family_overlap_allowed": True,
                    },
                }
            )
    value = {
        "schema_version": 1,
        "campaign_id": batch.CAMPAIGN_ID,
        "research_batch_id": batch.TARGET_BATCH_ID,
        "activation_policy": "ROLLING_TERMINAL_REPLACEMENT",
        "rolling_authorization_sha256": (
            batch.rolling_discovery_authorization.load_ready_status()[
                "authorization_sha256"
            ]
        ),
        "created_at": "2026-07-23T08:00:00-04:00",
        "families": families,
        "outcome_exposure_index_sha256": outcome_exposure.audit(index_path)[
            "index_sha256"
        ],
        "outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    value["inventory_sha256"] = contracts._hash(value)
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return path


def test_batch_freeze_fails_before_rolling_authorization(tmp_path):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    with pytest.raises(
        contracts.DenseFamilyContractError,
        match="not authorized before",
    ):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 22),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=tmp_path / "status.json",
            enforce_commit=False,
        )
    with pytest.raises(
        contracts.DenseFamilyContractError,
        match="cannot be future-dated",
    ):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 27),
            actual_today=date(2026, 7, 22),
            index_path=index,
            output_root=tmp_path / "future-contracts",
            status_path=tmp_path / "future-status.json",
            enforce_commit=False,
        )


def test_family_contract_cli_reports_fail_closed_json(monkeypatch, capsys):
    def fail(*_args, **_kwargs):
        raise contracts.DenseFamilyContractError("synthetic family blocker")

    monkeypatch.setattr(contracts, "freeze_batch", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dense_family_contracts.py",
            "missing-inventory.json",
            "--as-of",
            "2026-07-23",
        ],
    )

    assert contracts.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "error": "synthetic family blocker",
        "error_type": "DenseFamilyContractError",
    }


def test_batch_freezes_exact_three_valid_contracts_after_authorization(tmp_path):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    status_path = tmp_path / "status.json"
    batch.prepare(root=tmp_path / "plans", status_path=status_path)
    paths, status = contracts.freeze_batch(
        inventory,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=status_path,
        enforce_commit=False,
    )

    assert len(paths) == 3
    assert all(path.is_file() for path in paths)
    assert status["state"] == "THREE_FAMILY_CONTRACTS_FROZEN"
    assert status["family_contracts_frozen"] == 3
    assert status["provider_access_permitted"] is False
    assert status["outcome_access_permitted"] is False
    for path in paths:
        contract = json.loads(path.read_text(encoding="utf-8"))
        assert {
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        } <= set(contract["implementation_files"])
    assert json.loads(status_path.read_text(encoding="utf-8")) == status

    repeated_paths, repeated_status = contracts.freeze_batch(
        inventory,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=status_path,
        enforce_commit=False,
    )
    assert repeated_paths == paths
    assert repeated_status == status


def test_batch_freeze_rejects_unauthorized_status_transition_without_writes(
    tmp_path,
):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps(
            {
                    **contracts._zero_access_status(),
                "provider_access_permitted": True,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    output_root = tmp_path / "contracts"

    with pytest.raises(
        contracts.DenseFamilyContractError,
        match="exact authorized zero-access predecessor",
    ):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 23),
            actual_today=date(2026, 7, 23),
            index_path=index,
            output_root=output_root,
            status_path=status_path,
            enforce_commit=False,
        )

    assert not output_root.exists()


def test_batch_refreeze_supersedes_only_preflight_zero_access_status(
    tmp_path,
):
    index = tmp_path / "exposure.jsonl"
    status_path = tmp_path / "next_batch" / "status.json"
    first_inventory = _inventory(tmp_path, index)
    _paths, first_status = contracts.freeze_batch(
        first_inventory,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=status_path,
        enforce_commit=False,
    )
    outcome_exposure.append_record(
        outcome_exposure.build_record(
            exposure_id="unrelated-development-exposure",
            campaign_id="test",
            lane="development",
            recorded_at="2026-07-23T09:00:00-04:00",
            source_path="tests/source.json",
            source_sha256="b" * 64,
            scope={"dates": ["1999-01-04"], "symbols": ["TEST"]},
        ),
        index,
    )
    second_inventory = _inventory(tmp_path, index)

    _paths, second_status = contracts.freeze_batch(
        second_inventory,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=status_path,
        enforce_commit=False,
    )

    assert second_status["inventory_sha256"] != first_status["inventory_sha256"]
    assert second_status["superseded_zero_access_status"] == {
        "state": "THREE_FAMILY_CONTRACTS_FROZEN",
        "inventory_sha256": first_status["inventory_sha256"],
        "outcome_exposure_index_sha256": first_status[
            "outcome_exposure_index_sha256"
        ],
        "family_contract_sha256": first_status[
            "family_contract_sha256"
        ],
        "preflight_only_verified": True,
        "provider_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
    }


def test_batch_refreeze_rejects_status_after_search_artifact(tmp_path):
    index = tmp_path / "exposure.jsonl"
    status_path = tmp_path / "next_batch" / "status.json"
    first_inventory = _inventory(tmp_path, index)
    contracts.freeze_batch(
        first_inventory,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=status_path,
        enforce_commit=False,
    )
    search = (
        tmp_path
        / "discovery"
        / "liquid-equity-market-residual-reversal"
        / "search"
    )
    search.mkdir(parents=True)
    (search / "result.json").write_text("{}\n", encoding="utf-8")
    outcome_exposure.append_record(
        outcome_exposure.build_record(
            exposure_id="new-development-exposure",
            campaign_id="test",
            lane="development",
            recorded_at="2026-07-23T09:00:00-04:00",
            source_path="tests/source.json",
            source_sha256="c" * 64,
            scope={"dates": ["1999-01-05"], "symbols": ["TEST"]},
        ),
        index,
    )
    second_inventory = _inventory(tmp_path, index)

    with pytest.raises(
        contracts.DenseFamilyContractError,
        match="search or outcome artifacts",
    ):
        contracts.freeze_batch(
            second_inventory,
            as_of=date(2026, 7, 23),
            actual_today=date(2026, 7, 23),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=status_path,
            enforce_commit=False,
        )


def test_batch_freeze_requires_committed_inventory_after_authorization(
    tmp_path, monkeypatch
):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    checked = []

    def reject_uncommitted(path):
        checked.append(path)
        raise strategy_discovery.StrategyDiscoveryError("artifact is not committed")

    monkeypatch.setattr(strategy_discovery, "require_committed", reject_uncommitted)
    with pytest.raises(strategy_discovery.StrategyDiscoveryError, match="committed"):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 23),
            actual_today=date(2026, 7, 23),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=tmp_path / "status.json",
        )
    assert checked == [inventory]


def test_prior_outcome_pair_blocks_confirmation_scope(tmp_path):
    index = tmp_path / "exposure.jsonl"
    outcome_exposure.append_record(
        outcome_exposure.build_record(
            exposure_id="legacy-pair",
            campaign_id="legacy",
            lane="legacy",
            recorded_at="2026-07-22T19:00:00-04:00",
            source_path="PORTFOLIO_SIGNALS.jsonl",
            source_sha256="a" * 64,
            scope={"dates": ["2025-01-01"], "symbols": ["AAPL"]},
        ),
        index,
    )
    inventory = _inventory(tmp_path, index, overlap=True)
    with pytest.raises(contracts.DenseFamilyContractError, match="prior outcome"):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 23),
            actual_today=date(2026, 7, 23),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=tmp_path / "status.json",
            enforce_commit=False,
        )
