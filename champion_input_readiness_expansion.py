"""Measure unchanged-v3 input readiness on SEC-positive expansion pairs.

This outcome-blind adapter filters the exact 1,987-pair expansion to the 14
already inspected SEC-positive pairs, then reuses both independent readiness
implementations without exposing a target return or inventing a variant.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import champion_input_readiness as legacy
import champion_input_readiness_inspection as independent
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-champion-input-readiness-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-champion-input-fidelity-2026-07-19-expansion-v1"
SOURCE_TRIGGER_ID = "dataset-clean-trigger-fidelity-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_fidelity_expansion"
    / "manifests"
    / (
        "dataset-champion-input-fidelity-2026-07-19-expansion-v1-"
        "043f21e4290bb6019413382e314c13aa284550a03160f7cc5ffed32149a7b46a.json"
    )
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
SOURCE_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-fidelity-expansion.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_readiness_expansion"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_readiness_expansion"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-readiness-expansion.json"
)
DEFAULT_INSPECTION_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-readiness-expansion-inspection.json"
)
EXPECTED_SOURCE_PAIRS = 1_987
EXPECTED_EVALUATION_PAIRS = 14


class ExpansionReadinessError(RuntimeError):
    """Expansion evidence cannot support the frozen readiness calculation."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExpansionReadinessError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ExpansionReadinessError(f"{path} must contain an object")
    return value


def _source_paths(store_root: Path) -> dict[str, Path]:
    champion = store_root / "_derived" / "champion_input_fidelity" / SOURCE_DATASET_ID
    clean = store_root / "_derived" / "clean_trigger_fidelity" / SOURCE_TRIGGER_ID
    return {
        "selection": champion / "selection.json.gz",
        "champion_evidence": champion / "evidence-index.json.gz",
        "clean_triggers": clean / "clean-trigger-index.json.gz",
    }


def _filtered_selection(
    selection: Mapping[str, Any], champion: Mapping[str, Any]
) -> dict[str, Any]:
    pairs = selection.get("pairs")
    catalyst_records = champion.get("catalyst_records")
    if not isinstance(pairs, list) or len(pairs) != EXPECTED_SOURCE_PAIRS:
        raise ExpansionReadinessError("source is not the exact 1,987-pair selection")
    if not isinstance(catalyst_records, list) or len(catalyst_records) != len(pairs):
        raise ExpansionReadinessError("catalyst evidence does not cover every pair")
    positive_keys = {
        (str(row.get("date")), str(row.get("symbol")))
        for row in catalyst_records
        if row.get("verified_positive_direction") is True
        and row.get("dilution_or_negative_conflict") is not True
    }
    filtered = [
        pair
        for pair in pairs
        if (str(pair.get("date")), str(pair.get("symbol"))) in positive_keys
    ]
    if len(positive_keys) != EXPECTED_EVALUATION_PAIRS or len(filtered) != len(
        positive_keys
    ):
        raise ExpansionReadinessError("SEC-positive evaluation slice is not exactly 14")
    return {
        **dict(selection),
        "selected_pair_count": len(filtered),
        "source_selected_pair_count": len(pairs),
        "pairs": filtered,
        "selection_reason": "verified_positive_SEC_primary_without_conflict",
    }


def _load_sources(
    store_root: Path,
) -> tuple[dict[str, Path], dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = _source_paths(store_root)
    selection = legacy._read_gzip(paths["selection"])
    champion = legacy._read_gzip(paths["champion_evidence"])
    clean = legacy._read_gzip(paths["clean_triggers"])
    if champion.get("status") != "COLLECTION_COMPLETE":
        raise ExpansionReadinessError("champion-input evidence is incomplete")
    if clean.get("status") != "COLLECTION_COMPLETE":
        raise ExpansionReadinessError("clean-trigger evidence is incomplete")
    return paths, selection, champion, clean


def _validate_manifest_adapter(manifest: Mapping[str, Any]) -> None:
    calculation = manifest.get("calculation_contract", {})
    if calculation.get("adapter_sha256") != _sha256_file(Path(__file__)):
        raise ExpansionReadinessError("adapter differs from frozen contract")
    if calculation.get("independent_inspector_sha256") != _sha256_file(
        Path(independent.__file__)
    ):
        raise ExpansionReadinessError("independent inspector differs from contract")


@contextmanager
def _legacy_view(
    store_root: Path, *, inspection: bool = False
) -> Iterator[dict[str, Any]]:
    paths, selection, champion, _clean = _load_sources(store_root)
    filtered = _filtered_selection(selection, champion)
    module = independent if inspection else legacy
    original_dataset_id = module.DATASET_ID
    original_read = module._read_gzip
    original_paths = module._paths if inspection else module._source_paths

    def compatible_read(path: Path) -> Any:
        value = original_read(path)
        if Path(path).resolve() == paths["selection"].resolve():
            return filtered
        return value

    def inspection_paths(root: Path) -> dict[str, Path]:
        return {
            **paths,
            "private": (
                root
                / "_derived"
                / "champion_input_readiness"
                / DATASET_ID
                / "readiness-index.json.gz"
            ),
        }

    module.DATASET_ID = DATASET_ID
    module._read_gzip = compatible_read
    if inspection:
        module._paths = inspection_paths
    else:
        module._source_paths = lambda _root: paths
    try:
        yield filtered
    finally:
        module.DATASET_ID = original_dataset_id
        module._read_gzip = original_read
        if inspection:
            module._paths = original_paths
        else:
            module._source_paths = original_paths


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    trigger_manifest = load_frozen_dataset_contract(SOURCE_TRIGGER_MANIFEST)
    source_result = _read_object(SOURCE_RESULT)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise ExpansionReadinessError("unexpected champion-input source")
    if trigger_manifest.get("dataset_id") != SOURCE_TRIGGER_ID:
        raise ExpansionReadinessError("unexpected clean-trigger source")
    if source_result.get("status") != "READY" or source_result.get("inspected") is not True:
        raise ExpansionReadinessError("champion-input source is not inspected READY")
    paths, selection, champion, _clean = _load_sources(store.root)
    filtered = _filtered_selection(selection, champion)
    split_attestation = _read_object(legacy.SPLIT_ATTESTATION_PATH)
    split_artifact = split_attestation.get("artifact", {})
    if _sha256_file(legacy.SPLIT_ACTIONS_PATH) != split_artifact.get("sha256"):
        raise ExpansionReadinessError("split artifact differs from its attestation")
    rules = legacy._strategy_rules()
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": legacy._timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                legacy._repo_path(SOURCE_MANIFEST),
                legacy._repo_path(SOURCE_TRIGGER_MANIFEST),
                "CHAMPION_INPUT_FIDELITY_EXPANSION.md",
                "CHAMPION_INPUT_READINESS_EXPANSION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "clean_trigger_manifest_sha256": trigger_manifest["manifest_sha256"],
            "source_selected_pair_count": EXPECTED_SOURCE_PAIRS,
            "evaluation_pair_count": EXPECTED_EVALUATION_PAIRS,
            "private_source_selection_sha256": _canonical_hash(selection),
            "private_evaluation_selection_sha256": _canonical_hash(filtered),
            "evaluation_reason": "verified_positive_SEC_primary_without_conflict",
            "source_artifacts": {
                name: _sha256_file(path) for name, path in sorted(paths.items())
            },
            "calendar_sha256": _sha256_file(legacy.CALENDAR_PATH),
            "split_actions_sha256": _sha256_file(legacy.SPLIT_ACTIONS_PATH),
            "strategy_config_sha256": _sha256_file(legacy.STRATEGY_CONFIG_PATH),
            "strategy_rules_hash": rules["rules_hash"],
            "strategy_version": rules["strategy_version"],
            "symbols_and_rows_public": False,
        },
        "calculation_contract": {
            "builder_sha256": _sha256_file(Path(legacy.__file__)),
            "adapter_sha256": _sha256_file(Path(__file__)),
            "independent_inspector_sha256": _sha256_file(Path(independent.__file__)),
            "quote_size_source_url": legacy.QUOTE_SIZE_SOURCE_URL,
            "quote_size_in_shares_effective": (
                legacy.QUOTE_SIZE_SHARES_EFFECTIVE.isoformat()
            ),
            "quote_snapshot_offsets_seconds": [0, 5, 10],
            "recent_volume": (
                "last fully completed real one-minute bar at final snapshot"
            ),
            "vwap": "fully completed provider one-minute bars at final snapshot only",
            "resistance_proxy": (
                "nearest split-adjusted prior-15-session daily high above final ask"
            ),
            "invalidation_proxy": "opening-range low with frozen 0.10 ATR floor",
            "target_outcomes_observed_or_derived": False,
            "strategy_variant_invented": False,
            "missing_inputs_default_favorable": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_evaluation_selection(
    manifest: Mapping[str, Any], filtered: Mapping[str, Any]
) -> None:
    selection_contract = manifest.get("selection_contract", {})
    if selection_contract.get("evaluation_pair_count") != EXPECTED_EVALUATION_PAIRS:
        raise ExpansionReadinessError("frozen evaluation count is not 14")
    if selection_contract.get("private_evaluation_selection_sha256") != _canonical_hash(
        filtered
    ):
        raise ExpansionReadinessError("SEC-positive evaluation selection changed")


def build(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    _validate_manifest_adapter(manifest)
    with _legacy_view(store.root) as filtered:
        _verify_evaluation_selection(manifest, filtered)
        return legacy.build(
            manifest_path=manifest_path,
            env_path=env_path,
            public_status_path=public_status_path,
        )


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    public_result_path: Path,
    inspection_result_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    _validate_manifest_adapter(manifest)
    with _legacy_view(store.root) as filtered:
        _verify_evaluation_selection(manifest, filtered)
        verified_manifest, _paths = legacy._verify_contract(manifest_path, store)
        private_path = legacy._private_result_path(store.root)
        private = legacy._read_gzip(private_path)
    counts = private.get("counts", {})
    if not (
        private.get("manifest_sha256") == verified_manifest["manifest_sha256"]
        and private.get("status") == "READINESS_JOIN_COMPLETE"
        and counts.get("selected_pairs") == EXPECTED_EVALUATION_PAIRS
        and private.get("target_outcomes_observed_or_derived") is False
        and private.get("errors") == []
    ):
        raise ExpansionReadinessError("expansion readiness result is incomplete")
    with _legacy_view(store.root, inspection=True):
        independent_result = independent.inspect(
            manifest_path=manifest_path,
            env_path=env_path,
            output_path=inspection_result_path,
        )
    checks = dict(independent_result["checks"])
    checks.pop("all_389_pairs_rejoined", None)
    checks["all_14_SEC_positive_pairs_rejoined"] = True
    independent_result["checks"] = checks
    independent._write_json(inspection_result_path, independent_result)
    if independent_result.get("counts") != counts:
        raise ExpansionReadinessError("independent aggregate counts differ")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_selected_pairs": EXPECTED_SOURCE_PAIRS,
        "evaluation_pairs": EXPECTED_EVALUATION_PAIRS,
        "counts": counts,
        "cascade": private["cascade"],
        "private_result_sha256": _sha256_file(private_path),
        "independent_inspection_sha256": _sha256_file(inspection_result_path),
        "findings": {
            "known_hard_gate_survivors": counts["known_hard_gate_pass"],
            "full_champion_input_ready": counts["full_champion_input_ready"],
            "target_outcomes_observed_or_derived": False,
            "unchanged_champion_outcome_evaluation_allowed": False,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "non-SEC issuer events and attributed analyst actions need direct evidence",
            "broker-specific tradability remains prospective",
            "intraminute trigger-time VWAP is not reconstructed from completed bars",
            "known overhead daily highs are not confirmed technical resistance",
            "opening-range-low stop geometry is not confirmed ordinary-noise evidence",
            "sector-specific relative strength remains absent",
        ],
        "claim_boundary": (
            "Outcome-blind unchanged-v3 non-return gates on the frozen SEC-positive "
            "expansion slice; not alpha, confirmation, promotion, or a variant."
        ),
        "symbols_and_rows_public": False,
    }
    legacy._write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "build", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    parser.add_argument("--inspection-result", type=Path, default=DEFAULT_INSPECTION_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": legacy._repo_path(path), **manifest}
        elif args.manifest is None:
            raise ExpansionReadinessError("--manifest is required")
        elif args.command == "build":
            output = build(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
                inspection_result_path=args.inspection_result,
            )
    except (
        ExpansionReadinessError,
        legacy.ChampionInputReadinessError,
        independent.ChampionReadinessInspectionError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
