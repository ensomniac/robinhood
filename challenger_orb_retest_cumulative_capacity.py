"""Freeze the aggregate causal-signal capacity of the three ORB-retest corpora."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry as preentry
from historical_store import HistoricalStoreConfig, HistoricalStoreError


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-challenger-orb-retest-cumulative-trigger-capacity-2026-07-22-v1"
)
MINIMUM_DEVELOPMENT_SIGNALS = 50
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_preentry"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_cumulative_capacity_inspection.py"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "cumulative_capacity_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "cumulative-capacity-status.json"
)
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-cumulative-trigger-capacity.json"
)
SOURCE_SPECS = (
    {
        "corpus": "corpus_1",
        "dataset_id": (
            "dataset-challenger-orb-retest-preentry-collection-2026-07-21-v1"
        ),
        "result": PROJECT_ROOT
        / "research_results/2026-07-21-challenger-orb-retest-preentry.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1/trigger-status.json",
        "expected_trigger_dates": 22,
        "expected_trigger_pairs": 29,
        "expected_private_sha256": (
            "f8cf5c890eeae1bf9abef2c6a285d24574c182cbfca874bae3ff03c20ce0f7b4"
        ),
    },
    {
        "corpus": "corpus_2",
        "dataset_id": (
            "dataset-challenger-orb-retest-preentry-collection-2026-07-22-"
            "tranche2-v1"
        ),
        "result": PROJECT_ROOT
        / "research_results/2026-07-22-challenger-orb-retest-preentry-tranche2.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche2/trigger-status.json",
        "expected_trigger_dates": 22,
        "expected_trigger_pairs": 23,
        "expected_private_sha256": (
            "c6303e81763e25f2671fd343efaf9bfdf2a5b3c04425fb9953ee6494310cb4c3"
        ),
    },
    {
        "corpus": "corpus_3",
        "dataset_id": (
            "dataset-challenger-orb-retest-preentry-collection-2026-07-22-"
            "tranche3-v2"
        ),
        "result": PROJECT_ROOT
        / "research_results/2026-07-22-challenger-orb-retest-preentry-tranche3.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche3/trigger-status.json",
        "expected_trigger_dates": 8,
        "expected_trigger_pairs": 8,
        "expected_private_sha256": (
            "c03dd9c32df5758187f856fa3e69bbc40194561cb62568269c0832b5a03e73e3"
        ),
    },
)


class ChallengerCumulativeCapacityError(RuntimeError):
    """The cumulative capacity contract or one of its causal sources differs."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerCumulativeCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerCumulativeCapacityError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerCumulativeCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerCumulativeCapacityError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(dict(value), indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _private_trigger_path(store_root: Path, dataset_id: str) -> Path:
    return store_root / PRIVATE_NAMESPACE / dataset_id / "trigger-index.json.gz"


def _pairwise_counts(values: Sequence[set[str]]) -> list[int]:
    return [
        len(values[left].intersection(values[right]))
        for left, right in ((0, 1), (0, 2), (1, 2))
    ]


def build_capacity_state(
    store_root: Path,
) -> tuple[dict[str, Any], list[set[str]], list[set[str]]]:
    """Rebuild aggregate capacity while retaining exact dates only in memory."""

    sources: list[dict[str, Any]] = []
    trigger_sets: list[set[str]] = []
    corpus_sets: list[set[str]] = []
    for spec in SOURCE_SPECS:
        result_path = Path(spec["result"])
        status_path = Path(spec["status"])
        private_path = _private_trigger_path(store_root, str(spec["dataset_id"]))
        result = _read_json(result_path)
        status = _read_json(status_path)
        private = _read_gzip(private_path)
        counts = private.get("counts", {})
        records = private.get("records")
        if not isinstance(records, list):
            raise ChallengerCumulativeCapacityError("private trigger records are missing")
        private_sha256 = _sha256_file(private_path)
        if not (
            result.get("dataset_id") == spec["dataset_id"]
            and result.get("status") == "READY"
            and result.get("inspected") is True
            and result.get("valid") is True
            and result.get("private_result_sha256") == private_sha256
            and result.get("target_outcomes_observed_or_derived") is False
            and status.get("status") == "READY"
            and status.get("inspected") is True
            and status.get("private_trigger_sha256") == private_sha256
            and status.get("target_outcomes_observed_or_derived") is False
            and private.get("dataset_id") == spec["dataset_id"]
            and private.get("target_outcomes_observed_or_derived") is False
            and counts.get("trigger_found_dates")
            == spec["expected_trigger_dates"]
            and counts.get("trigger_found_pairs")
            == spec["expected_trigger_pairs"]
            and private_sha256 == spec["expected_private_sha256"]
        ):
            raise ChallengerCumulativeCapacityError(
                f"{spec['corpus']} inspected causal result differs"
            )
        corpus_dates = {str(row["date"]) for row in records}
        trigger_dates = {
            str(row["date"])
            for row in records
            if row.get("result", {}).get("terminal_reason") == "TRIGGER_FOUND"
        }
        trigger_pairs = sum(
            row.get("result", {}).get("terminal_reason") == "TRIGGER_FOUND"
            for row in records
        )
        if not (
            len(trigger_dates) == spec["expected_trigger_dates"]
            and trigger_pairs == spec["expected_trigger_pairs"]
            and len(corpus_dates) == counts.get("verified_positive_dates")
            and len(records) == counts.get("verified_positive_pairs")
        ):
            raise ChallengerCumulativeCapacityError(
                f"{spec['corpus']} private causal denominator differs"
            )
        sources.append(
            {
                "corpus": spec["corpus"],
                "dataset_id": spec["dataset_id"],
                "public_result_path": preentry._repo_path(result_path),
                "public_result_sha256": _sha256_file(result_path),
                "public_status_path": preentry._repo_path(status_path),
                "public_status_sha256": _sha256_file(status_path),
                "private_trigger_sha256": private_sha256,
                "source_sessions": len(corpus_dates),
                "source_pairs": len(records),
                "trigger_sessions": len(trigger_dates),
                "trigger_pairs": trigger_pairs,
                "private_source_session_set_sha256": _sha256_json(
                    sorted(corpus_dates)
                ),
                "private_trigger_session_set_sha256": _sha256_json(
                    sorted(trigger_dates)
                ),
            }
        )
        corpus_sets.append(corpus_dates)
        trigger_sets.append(trigger_dates)
    union = set().union(*trigger_sets)
    summary = {
        "schema_version": 1,
        "source_contracts": sources,
        "source_corpus_count": len(sources),
        "source_trigger_session_counts": [len(value) for value in trigger_sets],
        "source_trigger_pair_counts": [
            int(value["trigger_pairs"]) for value in sources
        ],
        "pairwise_source_session_intersections": _pairwise_counts(corpus_sets),
        "pairwise_trigger_session_intersections": _pairwise_counts(trigger_sets),
        "cumulative_distinct_trigger_sessions": len(union),
        "cumulative_trigger_pairs": sum(
            int(value["trigger_pairs"]) for value in sources
        ),
        "minimum_required_development_signals": MINIMUM_DEVELOPMENT_SIGNALS,
        "minimum_development_signal_capacity_passed": (
            len(union) >= MINIMUM_DEVELOPMENT_SIGNALS
        ),
        "private_trigger_session_union_sha256": _sha256_json(sorted(union)),
        "exact_dates_symbols_and_instrument_ids_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    return summary, trigger_sets, corpus_sets


def _implementation_contract() -> dict[str, dict[str, str]]:
    return {
        name: {"path": preentry._repo_path(path), "sha256": _sha256_file(path)}
        for name, path in {
            "freezer": Path(__file__),
            "inspector": INSPECTOR,
        }.items()
    }


def _manifest_fingerprint(value: Mapping[str, Any]) -> str:
    return _sha256_json(value)


def _write_manifest(
    value: Mapping[str, Any], output_root: Path
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("manifest_sha256", None)
    fingerprint = _manifest_fingerprint(content)
    frozen = {**content, "manifest_sha256": fingerprint}
    path = output_root / f"{DATASET_ID}-{fingerprint}.json"
    if path.exists() and _read_json(path) != frozen:
        raise ChallengerCumulativeCapacityError(
            "hash-addressed capacity manifest has other content"
        )
    _write_json(path, frozen)
    return path, frozen


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = _read_json(path)
    recorded = manifest.get("manifest_sha256")
    content = dict(manifest)
    content.pop("manifest_sha256", None)
    expected = _manifest_fingerprint(content)
    if not (
        manifest.get("schema_version") == 1
        and manifest.get("dataset_id") == DATASET_ID
        and recorded == expected
        and path.name == f"{DATASET_ID}-{expected}.json"
    ):
        raise ChallengerCumulativeCapacityError(
            "cumulative capacity manifest was mutated or renamed"
        )
    return manifest


def freeze_inputs(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (Path(__file__), INSPECTOR):
        preentry._published(path)
    for spec in SOURCE_SPECS:
        preentry._published(Path(spec["result"]))
        preentry._published(Path(spec["status"]))
    if DEFAULT_RESULT.exists():
        raise ChallengerCumulativeCapacityError(
            "cumulative capacity result exists before contract freeze"
        )
    config = HistoricalStoreConfig.from_env(env_path)
    summary, _trigger_sets, _corpus_sets = build_capacity_state(config.root)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "claim_scope": "DEVELOPMENT_ONLY",
        "calculation_contract": {
            "one_closed_signal_per_trigger_session": True,
            "pairwise_disjoint_source_sessions_required": True,
            "minimum_development_signals": MINIMUM_DEVELOPMENT_SIGNALS,
            "no_parameter_or_source_repair_allowed": True,
        },
        "capacity_snapshot": summary,
        "implementation_contract": _implementation_contract(),
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "quote_or_fill_access_allowed": False,
            "return_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "outcome_contract_must_be_separately_frozen": True,
        },
        "privacy_contract": {
            "exact_dates_symbols_and_instrument_ids_public": False,
            "aggregate_counts_and_hashes_only": True,
        },
    }
    path, manifest = _write_manifest(contract, output_root)
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_AWAITING_INSPECTION",
        "inspected": False,
        "counts": {
            "source_corpora": summary["source_corpus_count"],
            "source_trigger_sessions": summary["source_trigger_session_counts"],
            "cumulative_distinct_trigger_sessions": summary[
                "cumulative_distinct_trigger_sessions"
            ],
            "minimum_required_development_signals": MINIMUM_DEVELOPMENT_SIGNALS,
        },
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, status)
    return path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.status,
            )
            value = {"manifest": preentry._repo_path(path), **manifest}
        else:
            value = _read_json(args.status)
    except (
        ChallengerCumulativeCapacityError,
        HistoricalStoreError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
