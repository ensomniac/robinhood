"""Freeze and collect a distinct 2024 metadata backfill for PEAD development."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import earnings_gap_continuation as earnings
import earnings_pead_expansion as base
import strategy_discovery
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
BACKFILL_ID = "earnings-positive-surprise-drift-v3-2024-backfill"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / BACKFILL_ID
)
EVENT_START = "2024-01-01"
EVENT_END = "2024-12-31"
SOURCE_2026_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-positive-surprise-drift-v3-disjoint-2026/"
    "metadata-collection-inspection/"
    "inspection-c49265d39f51452312b22b85db3e7d5d50e4e95b43b726419c836d2ad117c607.json"
)


class EarningsPeadBackfillError(RuntimeError):
    """The distinct historical metadata backfill drifted."""


@contextmanager
def _configured():
    original = (
        base.EXPANSION_ID,
        base.DEFAULT_ROOT,
        base.EVENT_START,
        base.EVENT_END,
    )
    try:
        base.EXPANSION_ID = BACKFILL_ID
        base.DEFAULT_ROOT = DEFAULT_ROOT
        base.EVENT_START = EVENT_START
        base.EVENT_END = EVENT_END
        yield
    finally:
        (
            base.EXPANSION_ID,
            base.DEFAULT_ROOT,
            base.EVENT_START,
            base.EVENT_END,
        ) = original


def build_contract(*, created_at: str) -> dict[str, Any]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(SOURCE_2026_INSPECTION)
    with _configured():
        value = base.build_contract(created_at=created_at)
    value["artifact_kind"] = "earnings-pead-2024-backfill-contract"
    value["expansion_id"] = BACKFILL_ID
    value["event_start"] = EVENT_START
    value["event_end"] = EVENT_END
    value["selection_rule"] = (
        "Use the exact previously frozen 128-symbol universe and retain only "
        "unambiguous company-verified reports from 2024-01-01 through "
        "2024-12-31 with numeric actual EPS above estimated EPS. This is a "
        "distinct development target; it cannot alter late-2025 confirmation."
    )
    value["previous_2026_provider_requests"] = 128
    value["authorized_provider_requests"] = 128
    value["maximum_total_symbol_requests"] = 256
    value["source_2026_inspection_path"] = base._repo_path(
        SOURCE_2026_INSPECTION
    )
    value["source_2026_inspection_file_sha256"] = sha256_file(
        SOURCE_2026_INSPECTION
    )
    value["implementation_sha256"] = sha256_file(
        Path(__file__).resolve()
    )
    value["contract_sha256"] = earnings._self_hash(
        value, "contract_sha256"
    )
    return value


def freeze_contract(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "metadata-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    earnings._write_json(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    earnings._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    contract = base._read(contract_path)
    rebuilt = build_contract(created_at=contract["created_at"])
    checks = {
        "exact_rebuild": rebuilt == contract,
        "same_symbols": len(contract["symbols"]) == 128,
        "distinct_target": contract["event_end"] == EVENT_END,
        "total_request_cap": contract["maximum_total_symbol_requests"]
        == 256,
        "prior_trial_correction": contract[
            "prior_trials_must_enter_selection_correction"
        ]
        is True,
        "no_prices": contract["market_prices_accessed"] is False,
        "no_returns": contract["forward_returns_accessed"] is False,
        "no_broker": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsPeadBackfillError(
            "2024 metadata contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-pead-2024-backfill-contract-inspection",
        "campaign_id": base.CAMPAIGN_ID,
        "family_id": base.FAMILY_ID,
        "expansion_id": BACKFILL_ID,
        "state": "METADATA_CONTRACT_INSPECTED_READY",
        "inspected_at": inspected_at,
        "contract_path": base._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "provider_access_authorized": True,
        "authorized_provider_requests": 128,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = earnings._self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    earnings._write_json(path, value)
    return path, value


def ingest(
    contract_path: Path,
    inspection_path: Path,
    lines: Sequence[str],
    *,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    with _configured():
        return base.ingest(
            contract_path,
            inspection_path,
            lines,
            collected_at=collected_at,
            root=DEFAULT_ROOT,
        )


def inspect_collection(
    collection_path: Path, *, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    with _configured():
        return base.inspect_collection(
            collection_path,
            inspected_at=inspected_at,
            root=DEFAULT_ROOT,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    ingest_parser = sub.add_parser("ingest")
    ingest_parser.add_argument("contract", type=Path)
    ingest_parser.add_argument("inspection", type=Path)
    ingest_parser.add_argument("--collected-at", required=True)
    inspect_data = sub.add_parser("inspect-collection")
    inspect_data.add_argument("collection", type=Path)
    inspect_data.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
    elif args.command == "inspect-contract":
        path, value = inspect_contract(
            args.contract, inspected_at=args.inspected_at
        )
    elif args.command == "ingest":
        lines: list[str] = []
        for line in __import__("sys").stdin:
            lines.append(line)
            if line.strip() == "__END__":
                break
        path, value = ingest(
            args.contract,
            args.inspection,
            lines,
            collected_at=args.collected_at,
        )
    else:
        path, value = inspect_collection(
            args.collection, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": base._repo_path(path),
                "state": value.get("state", value["artifact_kind"]),
                "sha256": base._artifact_sha(value),
                "provider_requests": value.get("provider_requests", 0),
                "verified_positive_surprises": value.get(
                    "verified_positive_surprises", 0
                ),
                "market_prices_accessed": False,
                "forward_returns_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
