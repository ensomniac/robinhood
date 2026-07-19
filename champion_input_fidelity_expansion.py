"""Classify expansion SEC catalysts and join official halt state.

This adapter combines the complete SEC candidate index with the independently
inspected clean-trigger corpus.  It reuses the frozen primary-source classifier
and Nasdaq halt collector without reading any target outcome.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time as wall_time, timezone
from pathlib import Path
from typing import Any

import requests

import champion_input_fidelity as legacy_champion
from historical_discovery import HistoricalDiscoveryError
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from nasdaq_halts import NasdaqHaltError


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-champion-input-fidelity-2026-07-19-expansion-v1"
SOURCE_FIDELITY_ID = "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1"
SOURCE_TRIGGER_ID = "dataset-clean-trigger-fidelity-2026-07-19-expansion-v1"
SOURCE_FIDELITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1-"
        "ee9ba6f3a2d48f3337ee654c545ea3c02287e23f4a4db5a3fb86adf1b0c780ce.json"
    )
)
SOURCE_FIDELITY_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "collection-status.json"
)
SOURCE_TRIGGER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "clean_trigger_fidelity_expansion"
    / "manifests"
    / (
        "dataset-clean-trigger-fidelity-2026-07-19-expansion-v1-"
        "44225398ce0fde7acf19ddb6e87a0ace0195e19fed482d3258530f479e4d6ad4.json"
    )
)
SOURCE_TRIGGER_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-clean-trigger-fidelity-expansion.json"
)
CALENDAR_PATH = legacy_champion.CALENDAR_PATH
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_fidelity_expansion"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_fidelity_expansion"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-fidelity-expansion.json"
)


class ChampionExpansionError(RuntimeError):
    """The expansion champion-input contract or evidence is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionExpansionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChampionExpansionError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionExpansionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChampionExpansionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChampionExpansionError(f"path must be repository-relative: {path}") from exc


def _source_paths(store_root: Path) -> dict[str, Path]:
    fidelity = (
        store_root / "_derived" / "selected_candidate_fidelity" / SOURCE_FIDELITY_ID
    )
    trigger = store_root / "_derived" / "clean_trigger_fidelity" / SOURCE_TRIGGER_ID
    return {
        "selection": fidelity / "selection-and-cik-map.json.gz",
        "sec": fidelity / "primary-catalyst-index.json.gz",
        "trigger": trigger / "clean-trigger-index.json.gz",
    }


def _private_root(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return store_root / "_derived" / "champion_input_fidelity" / dataset_id


def _selection_path(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return _private_root(store_root, dataset_id) / "selection.json.gz"


def _result_path(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return _private_root(store_root, dataset_id) / "evidence-index.json.gz"


def _load_calendar() -> list[str]:
    value = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ChampionExpansionError("session calendar is malformed")
    return value


def _prior_close(day: str, calendar: list[str]) -> datetime:
    try:
        index = calendar.index(day)
    except ValueError as exc:
        raise ChampionExpansionError(f"calendar lacks target {day}") from exc
    if index < 1:
        raise ChampionExpansionError(f"calendar lacks prior session for {day}")
    return datetime.combine(
        date.fromisoformat(calendar[index - 1]),
        wall_time(16, 0),
        tzinfo=legacy_champion.EASTERN,
    )


def _validate_sources(env_path: Path) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    fidelity = load_frozen_dataset_contract(SOURCE_FIDELITY_MANIFEST)
    trigger = load_frozen_dataset_contract(SOURCE_TRIGGER_MANIFEST)
    fidelity_status = _read_object(SOURCE_FIDELITY_STATUS)
    trigger_result = _read_object(SOURCE_TRIGGER_RESULT)
    if (
        fidelity.get("dataset_id") != SOURCE_FIDELITY_ID
        or fidelity_status.get("manifest_sha256") != fidelity.get("manifest_sha256")
        or fidelity_status.get("status") != "SEC_COLLECTION_COMPLETE"
    ):
        raise ChampionExpansionError("source SEC evidence is not complete")
    if (
        trigger.get("dataset_id") != SOURCE_TRIGGER_ID
        or trigger_result.get("manifest_sha256") != trigger.get("manifest_sha256")
        or trigger_result.get("status") != "READY"
        or trigger_result.get("inspected") is not True
    ):
        raise ChampionExpansionError("source trigger evidence is not inspected READY")
    store_root = HistoricalStoreConfig.from_env(env_path).root
    paths = _source_paths(store_root)
    if not all(path.is_file() for path in paths.values()):
        raise ChampionExpansionError("source private artifacts are incomplete")
    source_sec = _read_gzip(paths["sec"])
    source_trigger = _read_gzip(paths["trigger"])
    if (
        _sha256_file(paths["sec"])
        != fidelity_status.get("private_sec_index_sha256")
        or _sha256_file(paths["trigger"])
        != trigger_result.get("private_trigger_index_sha256")
        or source_sec.get("status") != "SEC_COLLECTION_COMPLETE"
        or source_trigger.get("status") != "COLLECTION_COMPLETE"
    ):
        raise ChampionExpansionError("source private evidence differs")
    dependency = {
        "source_fidelity_manifest_sha256": fidelity["manifest_sha256"],
        "source_fidelity_status_sha256": _sha256_file(SOURCE_FIDELITY_STATUS),
        "source_trigger_manifest_sha256": trigger["manifest_sha256"],
        "source_trigger_result_sha256": _sha256_file(SOURCE_TRIGGER_RESULT),
        "source_artifacts": {
            name: _sha256_file(path) for name, path in sorted(paths.items())
        },
        "expected_selected_pairs": int(source_sec["counts"]["selected_pairs"]),
        "expected_trigger_windows": int(source_trigger["counts"]["crossing_windows"]),
    }
    return store_root, dependency, paths


def _matching_manifest(
    output_root: Path, dataset_id: str, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise ChampionExpansionError("champion expansion has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key)
            if not isinstance(observed, Mapping) or observed.get(
                "minimum_free_bytes"
            ) != value.get("minimum_free_bytes"):
                raise ChampionExpansionError("existing capacity contract differs")
            continue
        if manifest.get(key) != value:
            raise ChampionExpansionError(f"existing champion {key} differs")
    return matches[0], manifest


def freeze_contract(
    *, env_path: Path, output_root: Path, dataset_id: str = DATASET_ID
) -> tuple[Path, dict[str, Any]]:
    store_root, dependency, paths = _validate_sources(env_path)
    source_selection = _read_gzip(paths["selection"])
    calendar = _load_calendar()
    pairs = [
        {
            **dict(pair),
            "prior_session_close_et": _prior_close(str(pair["date"]), calendar).isoformat(),
        }
        for pair in source_selection["pairs"]
    ]
    private = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_fidelity_dataset_id": SOURCE_FIDELITY_ID,
        "source_trigger_dataset_id": SOURCE_TRIGGER_ID,
        "selected_pair_count": len(pairs),
        "pairs": pairs,
    }
    private_path = _selection_path(store_root, dataset_id)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise ChampionExpansionError("private champion selection changed")
    else:
        _write_gzip(private_path, private)
    free_bytes = shutil.disk_usage(store_root).free
    if free_bytes < legacy_champion.MINIMUM_FREE_BYTES:
        raise ChampionExpansionError("historical reserve is below 10 GiB")
    collection = {
        "adapter_sha256": _sha256_file(Path(__file__)),
        "collector_path": "champion_input_fidelity.py",
        "collector_sha256": _sha256_file(PROJECT_ROOT / "champion_input_fidelity.py"),
        "classifier_version": legacy_champion.CLASSIFIER_VERSION,
        "classifier_sha256": _sha256_file(PROJECT_ROOT / "primary_catalyst_evidence.py"),
        "halt_client_sha256": _sha256_file(PROJECT_ROOT / "nasdaq_halts.py"),
        "sec_source": "SEC EDGAR complete submission text and issuer exhibits",
        "catalyst_recency": "after prior regular-session close through target 09:35 ET",
        "halt_source": "Nasdaq Trader official historical halt RPC",
        "halt_window": "condition-valid clean cross through final +10 second quote target",
        "raw_and_symbol_rows_public": False,
        "target_outcomes_allowed": False,
        "alpha_or_confirmation_claim_allowed": False,
    }
    expected = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "requested_dates": load_frozen_dataset_contract(SOURCE_FIDELITY_MANIFEST)[
            "requested_dates"
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_FIDELITY_MANIFEST),
                _repo_path(SOURCE_FIDELITY_STATUS),
                _repo_path(SOURCE_TRIGGER_MANIFEST),
                _repo_path(SOURCE_TRIGGER_RESULT),
                "SCANNER_EXPANSION_FIDELITY.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            **dependency,
            "selected_pair_count": len(pairs),
            "private_selection_content_sha256": _sha256_json(private),
            "calendar_sha256": _sha256_file(CALENDAR_PATH),
            "symbols_ciks_and_filings_public": False,
        },
        "collection_contract": collection,
        "capacity_contract": {
            "minimum_free_bytes": legacy_champion.MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
        },
    }
    existing = _matching_manifest(output_root, dataset_id, expected)
    if existing is not None:
        return existing
    return freeze_dataset_contract(
        {**expected, "registered_at": datetime.now(timezone.utc).isoformat()},
        output_root,
    )


def _activate_legacy(manifest_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChampionExpansionError("unexpected champion expansion dataset")
    contract = manifest.get("collection_contract", {})
    if contract.get("adapter_sha256") != _sha256_file(Path(__file__)):
        raise ChampionExpansionError("champion expansion adapter changed")
    if contract.get("collector_sha256") != _sha256_file(
        PROJECT_ROOT / "champion_input_fidelity.py"
    ):
        raise ChampionExpansionError("legacy champion collector changed")
    legacy_champion.DATASET_ID = DATASET_ID
    legacy_champion.SOURCE_DATASET_ID = SOURCE_FIDELITY_ID
    legacy_champion._source_paths = _source_paths
    return manifest


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    manifest = _activate_legacy(manifest_path)
    store_root, dependency, _paths = _validate_sources(env_path)
    for key, value in dependency.items():
        if manifest["selection_contract"].get(key) != value:
            raise ChampionExpansionError(f"source dependency {key} differs")
    private_path = _result_path(store_root)
    private = _read_gzip(private_path)
    if private.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise ChampionExpansionError("private champion result is not manifest-bound")
    counts = dict(private.get("counts", {}))
    complete = (
        private.get("status") == "COLLECTION_COMPLETE"
        and int(counts.get("selected_pairs", 0))
        == int(manifest["selection_contract"]["expected_selected_pairs"])
        and int(counts.get("halt_dates_collected", 0))
        == len(manifest["requested_dates"])
        and int(counts.get("trigger_halt_windows_evaluated", 0))
        == int(manifest["selection_contract"]["expected_trigger_windows"])
        and private.get("errors") == []
    )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if complete else "INCOMPLETE",
        "inspected": complete,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_fidelity_dataset_id": SOURCE_FIDELITY_ID,
        "source_trigger_dataset_id": SOURCE_TRIGGER_ID,
        "counts": counts,
        "private_selection_content_sha256": manifest["selection_contract"][
            "private_selection_content_sha256"
        ],
        "private_result_sha256": _sha256_file(private_path),
        "findings": {
            "filing_presence_never_implies_positive_direction": True,
            "prior_close_recency_enforced": True,
            "official_halt_window_join_complete": counts.get(
                "trigger_halt_windows_evaluated"
            )
            == manifest["selection_contract"]["expected_trigger_windows"],
            "broker_specific_historical_tradability_available": False,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "non-SEC issuer events and analyst actions need direct-source corroboration",
            "broker-specific historical tradability requires prospective qualification",
            "resistance, stop invalidation/noise, and exact market alignment remain unresolved",
        ],
        "target_outcomes_observed_or_derived": False,
        "claim_boundary": (
            "Primary-source direction and halt fidelity only; not alpha, confirmation, "
            "maturity, promotion, or a strategy variant."
        ),
        "symbols_ciks_filings_and_raw_rows_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    for name in ("collect", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(
                env_path=args.env, output_root=args.output_root
            )
            result: Any = {
                "path": str(path),
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "selected_pair_count": manifest["selection_contract"][
                    "selected_pair_count"
                ],
                "expected_trigger_windows": manifest["selection_contract"][
                    "expected_trigger_windows"
                ],
            }
        elif args.command == "collect":
            _activate_legacy(args.manifest)
            result = legacy_champion.collect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        else:
            result = inspect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_result_path=DEFAULT_PUBLIC_RESULT,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChampionExpansionError,
        HistoricalDiscoveryError,
        HistoricalStoreError,
        LearningDataError,
        NasdaqHaltError,
        OSError,
        requests.RequestException,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
