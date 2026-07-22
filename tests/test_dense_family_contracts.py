from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

import dense_family_contracts as contracts
import next_week_discovery_batch as batch
import outcome_exposure
from learning_data import freeze_dataset_contract


def _dates(start: date, count: int) -> list[str]:
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _capacity_manifest(tmp_path, family_id: str, requested_dates: list[str]):
    path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-capacity-{family_id}",
            "registered_at": "2026-07-27T08:00:00-04:00",
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
        development = _dates(start, 60)
        embargo = _dates(start + timedelta(days=60), 5)
        confirmation = _dates(
            date(2025, 1, 1) if overlap and ordinal == 0 else start + timedelta(days=65),
            25,
        )
        requested = sorted({*development, *embargo, *confirmation})
        family_symbols = symbols[family["family_id"]]
        families.append(
            {
                "family_id": family["family_id"],
                "capacity_manifest": str(
                    _capacity_manifest(tmp_path, family["family_id"], requested)
                ),
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
            }
        )
    value = {
        "schema_version": 1,
        "campaign_id": batch.CAMPAIGN_ID,
        "target_iso_week": batch.TARGET_ISO_WEEK,
        "created_at": "2026-07-27T08:00:00-04:00",
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


def test_batch_freeze_fails_before_weekly_reset(tmp_path):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    with pytest.raises(contracts.DenseFamilyContractError, match="does not reset"):
        contracts.freeze_batch(
            inventory,
            as_of=date(2026, 7, 22),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=tmp_path / "status.json",
        )


def test_batch_freezes_exact_three_valid_contracts_after_reset(tmp_path):
    index = tmp_path / "exposure.jsonl"
    inventory = _inventory(tmp_path, index)
    paths, status = contracts.freeze_batch(
        inventory,
        as_of=date(2026, 7, 27),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
    )

    assert len(paths) == 3
    assert all(path.is_file() for path in paths)
    assert status["state"] == "THREE_FAMILY_CONTRACTS_FROZEN"
    assert status["family_contracts_frozen"] == 3
    assert status["provider_access_permitted"] is False
    assert status["outcome_access_permitted"] is False


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
            as_of=date(2026, 7, 27),
            index_path=index,
            output_root=tmp_path / "contracts",
            status_path=tmp_path / "status.json",
        )
