"""Independently inspect v7 reference collection and freeze listing identities."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import outcome_exposure
import residual_temporal_reference as reference
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_json_bytes,
    canonical_sha256,
    sha256_file,
)
from scanner_replay import (
    _instrument_id,
    _listing_scoped_instrument_id,
)


PROJECT_ROOT = Path(__file__).resolve().parent
INSPECTION_KIND = "residual-temporal-reference-inspection"
INSPECTION_STATE = "REFERENCE_IDENTITIES_INSPECTED"
FORBIDDEN_OUTCOME_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "price",
    "return",
    "forward_return",
    "volume",
    "vwap",
}


class ResidualTemporalReferenceInspectionError(RuntimeError):
    """The collected reference graph cannot support outcome-safe research."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualTemporalReferenceInspectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ResidualTemporalReferenceInspectionError(
            f"{field} needs a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualTemporalReferenceInspectionError(
            f"path escaped repository: {path}"
        ) from exc


def _read_snapshot(path: Path) -> list[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalReferenceInspectionError(
            f"cannot inspect reference snapshot {path}: {exc}"
        ) from exc
    if not isinstance(value, list) or not value:
        raise ResidualTemporalReferenceInspectionError(
            f"reference snapshot is empty: {path}"
        )
    if not all(isinstance(item, Mapping) for item in value):
        raise ResidualTemporalReferenceInspectionError(
            f"reference snapshot has malformed rows: {path}"
        )
    return [dict(item) for item in value]


def build_identities(
    snapshots: Mapping[str, Sequence[Mapping[str, Any]]]
) -> dict[str, dict[str, str]]:
    composite_listings: dict[str, dict[str, set[tuple[str, str]]]] = {}
    for day, rows in snapshots.items():
        listings: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for row in rows:
            identity, _source = _instrument_id(row)
            symbol = str(row.get("ticker") or "").strip().upper()
            exchange = str(row.get("primary_exchange") or "").strip().upper()
            if identity.startswith("FIGI-COMPOSITE:"):
                listings[identity].add((symbol, exchange))
        composite_listings[day] = listings

    output: dict[str, dict[str, str]] = {}
    for day in sorted(snapshots):
        by_symbol: dict[str, str] = {}
        for row in snapshots[day]:
            if FORBIDDEN_OUTCOME_FIELDS & {
                str(field).strip().lower() for field in row
            }:
                raise ResidualTemporalReferenceInspectionError(
                    f"{day}: reference row contains a price or outcome field"
                )
            provider_symbol = str(row.get("ticker") or "").strip()
            if provider_symbol != provider_symbol.upper():
                continue
            symbol = provider_symbol
            exchange = str(row.get("primary_exchange") or "").strip().upper()
            security_type = str(row.get("type") or "").strip().upper()
            active = row.get("active")
            if (
                exchange not in {"XNAS", "XNYS"}
                or security_type != "CS"
                or active is not True
            ):
                continue
            identity, _source = _instrument_id(row)
            if len(composite_listings[day].get(identity, set())) > 1:
                identity = _listing_scoped_instrument_id(
                    identity, symbol, exchange
                )
            if not symbol or symbol in by_symbol:
                raise ResidualTemporalReferenceInspectionError(
                    f"{day}: common-stock symbol identity is ambiguous"
                )
            by_symbol[symbol] = identity
        if len(by_symbol) < 500 or len(set(by_symbol.values())) != len(
            by_symbol
        ):
            raise ResidualTemporalReferenceInspectionError(
                f"{day}: common-stock identity denominator is incomplete"
            )
        output[day] = dict(sorted(by_symbol.items()))
    return output


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=temporary.open("wb"), mtime=0
        ) as target:
            target.write(canonical_json_bytes(value) + b"\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def inspect(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = reference.ROOT,
    store_config: HistoricalStoreConfig | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    observed_at = _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(collection_path)
    collection = strategy_discovery.load_artifact(
        collection_path, expected_kind=reference.COLLECTION_KIND
    )
    completed_at = _timestamp(
        str(collection["collection_completed_at"]), "collection_completed_at"
    )
    if observed_at <= completed_at:
        raise ResidualTemporalReferenceInspectionError(
            "reference inspection must follow collection"
        )
    contract_path = PROJECT_ROOT / str(collection["contract_path"])
    contract = reference.load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    if not (
        collection.get("state") == reference.COLLECTION_STATE
        and collection.get("contract_sha256") == contract["artifact_sha256"]
        and collection.get("prices_or_returns_accessed") is False
        and collection.get("strategy_outcomes_accessed") is False
        and collection.get("substitutions") == 0
        and collection.get("broker_actions") == 0
    ):
        raise ResidualTemporalReferenceInspectionError(
            "reference collection state is unsafe"
        )
    store = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    snapshot_root = (
        store.root / str(collection["external_relative_path"])
    ).resolve()
    if store.root.resolve() not in snapshot_root.parents:
        raise ResidualTemporalReferenceInspectionError(
            "reference collection escaped the historical store"
        )
    expected_dates = reference.selected_dates()
    snapshots: dict[str, list[dict[str, Any]]] = {}
    rebuilt_manifest: list[dict[str, Any]] = []
    declared = {
        str(item["date"]): item for item in collection.get("snapshots", [])
    }
    for day in expected_dates:
        path = snapshot_root / f"{day}.json.gz"
        if day not in declared or not path.is_file():
            raise ResidualTemporalReferenceInspectionError(
                f"{day}: reference snapshot is missing"
            )
        rows = _read_snapshot(path)
        observed_sha = sha256_file(path)
        if (
            observed_sha != declared[day].get("sha256")
            or len(rows) != declared[day].get("rows")
        ):
            raise ResidualTemporalReferenceInspectionError(
                f"{day}: reference snapshot binding drifted"
            )
        snapshots[day] = rows
        rebuilt_manifest.append(
            {
                "date": day,
                "rows": len(rows),
                "sha256": observed_sha,
                "disposition": declared[day].get("disposition"),
            }
        )
    identities = build_identities(snapshots)
    excluded_noncanonical_symbol_pairs = sum(
        1
        for rows in snapshots.values()
        for row in rows
        if (
            str(row.get("ticker") or "").strip()
            != str(row.get("ticker") or "").strip().upper()
        )
    )
    records = outcome_exposure.read_index()
    exact_scope = {
        "dates": expected_dates,
        "symbols_by_date": {
            day: sorted(identities[day]) for day in expected_dates
        },
    }
    overlaps = outcome_exposure.find_overlaps(exact_scope, records)
    if overlaps:
        raise ResidualTemporalReferenceInspectionError(
            f"early common-stock scope has {len(overlaps)} prior exposures"
        )
    outcome_exposure.assert_untouched(
        contract["preserved_confirmation"]["scope"], records
    )
    identity_relative = (
        Path("_derived")
        / reference.SUCCESSOR_ID
        / "identities"
        / f"identities-{canonical_sha256(identities)}.json.gz"
    )
    identity_path = store.root / identity_relative
    if identity_path.exists():
        try:
            with gzip.open(identity_path, "rt", encoding="utf-8") as source:
                existing = json.load(source)
        except (OSError, json.JSONDecodeError) as exc:
            raise ResidualTemporalReferenceInspectionError(
                "cached identity graph is unreadable"
            ) from exc
        if canonical_sha256(existing) != canonical_sha256(identities):
            raise ResidualTemporalReferenceInspectionError(
                "immutable identity graph drifted"
            )
    else:
        _write_gzip(identity_path, identities)
    counts = [len(identities[day]) for day in expected_dates]
    checks = {
        "contract_hash_valid": collection.get("contract_sha256")
        == contract["artifact_sha256"],
        "dates_rebuilt": expected_dates
        == contract["selection_contract"]["requested_dates"],
        "snapshot_count_complete": len(snapshots) == reference.REFERENCE_DATE_COUNT,
        "snapshot_manifest_rebuilt": canonical_sha256(rebuilt_manifest)
        == collection["snapshot_manifest_sha256"],
        "minimum_identity_denominator": min(counts) >= 500,
        "exact_scope_untouched": not overlaps,
        "confirmation_still_untouched": not outcome_exposure.find_overlaps(
            contract["preserved_confirmation"]["scope"], records
        ),
        "prices_or_returns_absent": collection.get(
            "prices_or_returns_accessed"
        )
        is False,
        "substitutions_zero": collection.get("substitutions") == 0,
        "broker_actions_zero": collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise ResidualTemporalReferenceInspectionError(
            "reference inspection failed: " + ", ".join(failed)
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": INSPECTION_KIND,
        "state": INSPECTION_STATE,
        "campaign_id": reference.CAMPAIGN_ID,
        "family_id": reference.FAMILY_ID,
        "collection_path": _repo_path(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "date_count": len(expected_dates),
        "minimum_identities_per_date": min(counts),
        "maximum_identities_per_date": max(counts),
        "identity_pair_count": sum(counts),
        "excluded_noncanonical_symbol_pairs": (
            excluded_noncanonical_symbol_pairs
        ),
        "exact_development_scope_sha256": canonical_sha256(exact_scope),
        "identity_graph_sha256": canonical_sha256(identities),
        "identity_external_relative_path": str(identity_relative),
        "identity_external_file_sha256": sha256_file(identity_path),
        "market_prices_accessed": False,
        "strategy_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "reference-inspection",
        "residual-temporal-reference-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--inspected-at", required=True)
    parser.add_argument("--root", type=Path, default=reference.ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, artifact = inspect(
            args.collection,
            inspected_at=args.inspected_at,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "date_count": artifact["date_count"],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualTemporalReferenceInspectionError,
        reference.ResidualTemporalReferenceError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
