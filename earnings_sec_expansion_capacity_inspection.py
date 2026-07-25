"""Independently inspect the 2012-2019 SEC PEAD capacity freeze."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_expansion_capacity as source
import outcome_exposure
import strategy_discovery
from historical_store import sha256_file


class EarningsSecExpansionInspectionError(RuntimeError):
    """The SEC PEAD expansion contract failed independent reconstruction."""


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = source._read(contract_path)
    rebuilt = source.build_contract(created_at=str(contract["created_at"]))
    request_rows = contract.get("requests", [])
    checks = {
        "artifact_hash_valid": contract.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
        "exact_contract_rebuild": contract == rebuilt,
        "exact_32_archives": request_rows == source.requests()
        and len(request_rows) == 32,
        "request_hashes_valid": all(
            row.get("request_sha256")
            == hashlib.sha256(
                source.canonical_bytes(
                    {
                        key: item
                        for key, item in row.items()
                        if key != "request_sha256"
                    }
                )
            ).hexdigest()
            for row in request_rows
        ),
        "calendar_wait_absent": contract.get("rolling_authority", {}).get(
            "calendar_wait_required"
        )
        is False,
        "existing_family_slot_preserved": contract.get(
            "rolling_authority", {}
        ).get("new_mechanism_family_slot_consumed")
        is False,
        "point_in_time_identity": contract.get("event_semantics", {}).get(
            "external_or_current_ticker_mapping_permitted"
        )
        is False,
        "same_accession_common_equity_gate": contract.get(
            "event_semantics", {}
        ).get("same_accession_common_stock_shares_cover_fact_required")
        is True,
        "partitions_exact": contract.get("partitions", {}).get("development")
        == [source.DEVELOPMENT_START, source.DEVELOPMENT_END]
        and contract.get("partitions", {}).get("embargo")
        == [source.EMBARGO_START, source.EMBARGO_END]
        and contract.get("partitions", {}).get("confirmation")
        == [source.CONFIRMATION_START, source.CONFIRMATION_END],
        "five_session_hold_and_embargo": contract.get(
            "partitions", {}
        ).get("maximum_hold_sessions")
        == 5
        and contract.get("partitions", {}).get(
            "five_complete_session_embargo_required"
        )
        is True,
        "cumulative_selection_accounting": contract.get(
            "selection_accounting", {}
        ).get("prior_evaluated_trials_same_mechanism")
        == source.PRIOR_EVALUATED_TRIALS
        and contract.get("selection_accounting", {}).get(
            "prior_trials_must_enter_deflated_sharpe_correction"
        )
        is True,
        "outcome_index_current": contract.get(
            "outcome_exposure_index_sha256"
        )
        == outcome_exposure.audit()["index_sha256"],
        "zero_provider_access": contract.get("provider_requests_executed") == 0
        and contract.get("metadata_rows_accessed") == 0,
        "market_prices_absent": contract.get("market_prices_accessed") is False,
        "forward_returns_absent": contract.get("forward_returns_accessed")
        is False,
        "strategy_metrics_absent": contract.get(
            "strategy_metrics_computed"
        )
        == 0,
        "confirmation_sealed": contract.get(
            "confirmation_outcomes_accessed"
        )
        is False,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecExpansionInspectionError(
            "SEC PEAD expansion contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-expansion-capacity-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_CAPACITY_CONTRACT_INSPECTED_READY",
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "authorized_metadata_requests": len(source.ARCHIVES),
        "provider_access_authorized": True,
        "market_price_access_authorized": False,
        "confirmation_access_authorized": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect a committed SEC PEAD expansion capacity freeze."
    )
    parser.add_argument("command", choices=("inspect-contract",))
    parser.add_argument("contract", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_contract(
        args.contract, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": source._repo_path(path),
                "inspection_sha256": value["inspection_sha256"],
                "state": value["state"],
                "valid": value["valid"],
                "market_price_access_authorized": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
